
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

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.hdfs.server.namenode.ha.ObserverReadProxyProvider.AUTO_MSYNC_PERIOD_KEY_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.namenode.ha.ObserverReadProxyProvider;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.ipc.NameServiceStateIdMode;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Test;

public class TestObserverWithRouter {

  public static final String FEDERATION_NS = "ns-fed";
  public static final int LOOPS = 10;

  private MiniRouterDFSCluster cluster;

  public void startUpCluster(int numberOfObserver) throws Exception {
    startUpCluster(numberOfObserver, NameServiceStateIdMode.PROXY, null);
  }

  public void startUpCluster(int numberOfObserver, NameServiceStateIdMode mode,
                             Configuration confOverrides) throws Exception {
    int numberOfNamenode = 2 + numberOfObserver;
    Configuration conf = new Configuration(false);
    conf.setInt(RBFConfigKeys.DFS_ROUTER_OBSERVER_AUTO_MSYNC_PERIOD, 0);
    conf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY, true);
    conf.set(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, "0ms");
    conf.setInt(DFSConfigKeys.DFS_ESTIMATED_SERVER_TIME_MULTIPLIER_KEY, 0);
    conf.setInt(DFS_ROUTER_HEARTBEAT_INTERVAL_MS, 1);
    conf.set(RBFConfigKeys.DFS_ROUTER_NAMESERVICE_STATE_ID_MODE, mode.name());

    if (confOverrides != null) {
      conf.addResource(confOverrides);
    }
    cluster = new MiniRouterDFSCluster(true, 2, numberOfNamenode);
    cluster.addNamenodeOverrides(conf);
    // Start NNs and DNs and wait until ready
    cluster.startCluster();

    // Making one Namenodes active per nameservice
    if (cluster.isHighAvailability()) {
      for (String ns : cluster.getNameservices()) {
        cluster.switchToActive(ns, NAMENODES[0]);
        cluster.switchToStandby(ns, NAMENODES[1]);
        for (int i = 2; i < numberOfNamenode; i++) {
          cluster.switchToObserver(ns, NAMENODES[i]);
        }
      }
    }

    Configuration routerConf = new RouterConfigBuilder()
        .metrics()
        .rpc()
        .build();

    cluster.addRouterOverrides(conf);
    cluster.addRouterOverrides(routerConf);

    // Start routers with only an RPC service
    cluster.startRouters();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();
    // Setup the mount table
    cluster.installMockLocations();

    cluster.waitActiveNamespaces();
  }

  @After
  public void teardown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    RouterStateIdCache.clear();
  }

  @Test(timeout = 600000)
  public void testObserveRead() throws Exception {
    /*  TestCase 1
     *  If the NameserviceStateIdStateIdMode of router is DISABLE,
     *  whatever the NameserviceStateIdStateIdMode and provider of client is,
     *  router will connect active namenode directly!
     *  In this unit-test, we will do below steps:
     *   (1) fs1 create   Router will just connect active
     *   (2) fs1 read     Router will just connect active
     *   (3) fs2 create   Router will just connect active
     *   (4) stop observer EditLogTailer.
     *   (5) fs1 read    Router will just connect active.
     *   active ops is 2 (fs1's create, complete) * LOOP
     *               + 1 (fs1's open) * LOOP
     *               + 2 (fs2's create, complete) * LOOP
     *               + 1 (fs1's open) * LOOP
     * */
    testObserveRead(NameServiceStateIdMode.DISABLE, NameServiceStateIdMode.DISABLE,
        ObserverReadProxyProvider.class, 6 * LOOPS, 0 * LOOPS, false, 1);
    testObserveRead(NameServiceStateIdMode.TRANSMISSION, NameServiceStateIdMode.DISABLE,
        ObserverReadProxyProvider.class, 6 * LOOPS, 0 * LOOPS, false, 1);
    testObserveRead(NameServiceStateIdMode.PROXY, NameServiceStateIdMode.DISABLE,
        ObserverReadProxyProvider.class, 6 * LOOPS, 0 * LOOPS, false, 1);
    testObserveRead(NameServiceStateIdMode.TRANSMISSION, NameServiceStateIdMode.DISABLE,
        ConfiguredFailoverProxyProvider.class, 6 * LOOPS, 0 * LOOPS, false, 1);
    testObserveRead(NameServiceStateIdMode.PROXY, NameServiceStateIdMode.DISABLE,
        ConfiguredFailoverProxyProvider.class, 6 * LOOPS, 0 * LOOPS, false, 1);

    /*  TestCase 2
     *  If the NameserviceStateIdStateIdMode of router is PROXY,
     *  whatever the NameserviceStateIdStateIdMode and provider of client is,
     *  router will connect observe with state id managed by router.
     *  In this unit-test, we will do below steps:
     *   (1) fs1 create   Router will just connect active
     *   (2) fs1 read     Router will connect observer because state is update in step (1)
     *   (3) fs2 create   Router will just connect active
     *   (4) stop observer EditLogTailer.
     *   (5) fs1 read     Router will connect active. the state manged by router is updated in
     *                    step (3), but observer is far behind. So router will failover to active.
     *  active ops is 2 (fs1's create, complete) * LOOP
     *              + 1 (fs1's msync) * LOOP
     *              + 2 (fs2's create, complete) * LOOP
     *              + 2 (fs1's open + msync) * LOOP
     *  observer ops is 1 (fs1's open) * LOOP
     * */
    testObserveRead(NameServiceStateIdMode.DISABLE, NameServiceStateIdMode.PROXY,
        ObserverReadProxyProvider.class, 7 * LOOPS, 1 * LOOPS, false, 1);
    testObserveRead(NameServiceStateIdMode.PROXY, NameServiceStateIdMode.PROXY,
        ObserverReadProxyProvider.class, 7 * LOOPS, 1 * LOOPS, false, 1);
    testObserveRead(NameServiceStateIdMode.TRANSMISSION, NameServiceStateIdMode.PROXY,
        ObserverReadProxyProvider.class, 7 * LOOPS, 1 * LOOPS, false, 1);
    testObserveRead(NameServiceStateIdMode.PROXY, NameServiceStateIdMode.PROXY,
        ConfiguredFailoverProxyProvider.class, 7 * LOOPS, 1 * LOOPS, false, 1);
    testObserveRead(NameServiceStateIdMode.TRANSMISSION, NameServiceStateIdMode.PROXY,
        ConfiguredFailoverProxyProvider.class, 7 * LOOPS, 1 * LOOPS, false, 1);

    /*  TestCase 3
     *  If the NameserviceStateIdStateIdMode of router is TRANSMISSION,
     *  The behavior of the router depend on client's NameserviceStateIdStateIdMode.
     *  TestCase 3.1
     *  If the NameserviceStateIdStateIdMode of client is DISABLE or provider is
     *  nonObserverReadProxyProvider, router will connect active namenode directly!
     *  In this unit-test, we will do below steps:
     *   (1) fs1 create   Router will just connect active
     *   (2) fs1 read     Router will just connect active
     *   (3) fs2 create   Router will just connect active
     *   (4) stop observer EditLogTailer.
     *   (5) fs1 read    Router will just connect active.
     *   active ops is 2 (fs1's create, complete) * LOOP
     *               + 1 (fs1's open) * LOOP
     *               + 2 (fs2's create, complete) * LOOP
     *               + 1 (fs1's open) * LOOP
     * */
    testObserveRead(NameServiceStateIdMode.DISABLE, NameServiceStateIdMode.TRANSMISSION,
        ObserverReadProxyProvider.class, 6 * LOOPS, 0 * LOOPS, false, 1);
    testObserveRead(NameServiceStateIdMode.PROXY, NameServiceStateIdMode.TRANSMISSION,
        ConfiguredFailoverProxyProvider.class, 6 * LOOPS, 0 * LOOPS, false, 1);
    testObserveRead(NameServiceStateIdMode.TRANSMISSION,
        NameServiceStateIdMode.TRANSMISSION, ConfiguredFailoverProxyProvider.class, 6 * LOOPS,
        0 * LOOPS, false, 1);

    /*  TestCase 3.2
     *  If the NameserviceStateIdStateIdMode of client is PROXY,
     *  router will connect observe with state id managed by router.
     *  In this unit-test, we will do below steps:
     *   (1) fs1 create   Router will just connect active
     *   (2) fs1 read     Router will connect observer because state is update in step (1)
     *   (3) fs2 create   Router will just connect active
     *   (4) stop observer EditLogTailer.
     *   (5) fs1 read     Router will connect active. the state manged by router is updated in
     *                    step (3), but observer is far behind. So router will failover to active.
     *  active ops is 2 (fs1's create, complete) * LOOP
     *              + 1 (fs1's msync) * LOOP
     *              + 2 (fs2's create, complete) * LOOP
     *              + 2 (fs1's open + msync) * LOOP
     *  observer ops is 1 (fs1's open) * LOOP
     * */
    testObserveRead(NameServiceStateIdMode.PROXY, NameServiceStateIdMode.TRANSMISSION,
        ObserverReadProxyProvider.class, 7 * LOOPS, 1 * LOOPS, false, 1);

    /*  TestCase 3.3
     *  If the NameserviceStateIdStateIdMode of client is TRANSMISSION,
     *  router will connect observe with state id managed by client.
     *  In this unit-test, we will do below steps:
     *   (1) fs1 create   Router will just connect active
     *   (2) fs1 read     Router will connect observer because state is update in step (1)
     *   (3) fs2 create   Router will just connect active
     *   (4) stop observer EditLogTailer.
     *   (5) fs1 read     Router will connect observer. Though observer is far behind, fs use their
     *                    own state id, observer will not reject fs1.
     *  active ops is 2 (fs1's create, complete) * LOOP
     *              + 2 (fs2's create, complete) * LOOP
     *  observer ops is 1 (fs1's open) * LOOP
     *                + 1 (fs3's open) * LOOP
     * */
    testObserveRead(NameServiceStateIdMode.TRANSMISSION, NameServiceStateIdMode.TRANSMISSION,
        ObserverReadProxyProvider.class, 4 * LOOPS, 2 * LOOPS, false, 1);
  }

  @Test(timeout = 600000)
  public void testMultipleObserveReadWithRouter() throws Exception {
    /*  TestCase 1
     *  If the NameserviceStateIdStateIdMode of router is DISABLE,
     *  whatever the NameserviceStateIdStateIdMode and provider of client is,
     *  router will connect active namenode directly!
     *  In this unit-test, we will do below steps:
     *   (1) fs1 create   Router will just connect active
     *   (2) fs1 read     Router will just connect active
     *   (3) fs2 create   Router will just connect active
     *   (4) stop observer EditLogTailer.
     *   (5) fs1 read    Router will just connect active.
     *   active ops is 2 (fs1's create, complete) * LOOP
     *               + 1 (fs1's open) * LOOP
     *               + 2 (fs2's create, complete) * LOOP
     *               + 1 (fs1's open) * LOOP
     * */
    testObserveRead(NameServiceStateIdMode.DISABLE, NameServiceStateIdMode.DISABLE,
        ObserverReadProxyProvider.class, 6 * LOOPS, 0 * LOOPS, false, 2);
    testObserveRead(NameServiceStateIdMode.TRANSMISSION, NameServiceStateIdMode.DISABLE,
        ObserverReadProxyProvider.class, 6 * LOOPS, 0 * LOOPS, false, 2);
    testObserveRead(NameServiceStateIdMode.PROXY, NameServiceStateIdMode.DISABLE,
        ObserverReadProxyProvider.class, 6 * LOOPS, 0 * LOOPS, false, 2);
    testObserveRead(NameServiceStateIdMode.TRANSMISSION, NameServiceStateIdMode.DISABLE,
        ConfiguredFailoverProxyProvider.class, 6 * LOOPS, 0 * LOOPS, false, 2);
    testObserveRead(NameServiceStateIdMode.PROXY, NameServiceStateIdMode.DISABLE,
        ConfiguredFailoverProxyProvider.class, 6 * LOOPS, 0 * LOOPS, false, 2);

    /*  TestCase 2
     *  If the NameserviceStateIdStateIdMode of router is PROXY,
     *  whatever the NameserviceStateIdStateIdMode and provider of client is,
     *  router will connect observe with state id managed by router.
     *  In this unit-test, we will do below steps:
     *   (1) fs1 create   Router will just connect active
     *   (2) fs1 read     Router will connect observer because state is update in step (1)
     *   (3) fs2 create   Router will just connect active
     *   (4) stop observer EditLogTailer.
     *   (5) fs1 read     Router will connect active. the state manged by router is updated in
     *                    step (3), but observer is far behind. So router will failover to active.
     *  active ops is 2 (fs1's create, complete) * LOOP
     *              + 1 (fs1's msync) * LOOP
     *              + 2 (fs2's create, complete) * LOOP
     *              + 2 (fs1's open + msync) * LOOP
     *  observer ops is 1 (fs1's open) * LOOP
     * */
    testObserveRead(NameServiceStateIdMode.DISABLE, NameServiceStateIdMode.PROXY,
        ObserverReadProxyProvider.class, 7 * LOOPS, 1 * LOOPS, false, 2);
    testObserveRead(NameServiceStateIdMode.PROXY, NameServiceStateIdMode.PROXY,
        ObserverReadProxyProvider.class, 7 * LOOPS, 1 * LOOPS, false, 2);
    testObserveRead(NameServiceStateIdMode.TRANSMISSION, NameServiceStateIdMode.PROXY,
        ObserverReadProxyProvider.class, 7 * LOOPS, 1 * LOOPS, false, 2);
    testObserveRead(NameServiceStateIdMode.PROXY, NameServiceStateIdMode.PROXY,
        ConfiguredFailoverProxyProvider.class, 7 * LOOPS, 1 * LOOPS, false, 2);
    testObserveRead(NameServiceStateIdMode.TRANSMISSION, NameServiceStateIdMode.PROXY,
        ConfiguredFailoverProxyProvider.class, 7 * LOOPS, 1 * LOOPS, false, 2);

    /*  TestCase 3
     *  If the NameserviceStateIdStateIdMode of router is TRANSMISSION,
     *  The behavior of the router depend on client's NameserviceStateIdStateIdMode.
     *  TestCase 3.1
     *  If the NameserviceStateIdStateIdMode of client is DISABLE or provider is
     *  nonObserverReadProxyProvider, router will connect active namenode directly!
     *  In this unit-test, we will do below steps:
     *   (1) fs1 create   Router will just connect active
     *   (2) fs1 read     Router will just connect active
     *   (3) fs2 create   Router will just connect active
     *   (4) stop observer EditLogTailer.
     *   (5) fs1 read    Router will just connect active.
     *   active ops is 2 (fs1's create, complete) * LOOP
     *               + 1 (fs1's open) * LOOP
     *               + 2 (fs2's create, complete) * LOOP
     *               + 1 (fs1's open) * LOOP
     * */
    testObserveRead(NameServiceStateIdMode.DISABLE, NameServiceStateIdMode.TRANSMISSION,
        ObserverReadProxyProvider.class, 6 * LOOPS, 0 * LOOPS, false, 2);
    testObserveRead(NameServiceStateIdMode.PROXY, NameServiceStateIdMode.TRANSMISSION,
        ConfiguredFailoverProxyProvider.class, 6 * LOOPS, 0 * LOOPS, false, 2);
    testObserveRead(NameServiceStateIdMode.TRANSMISSION,
        NameServiceStateIdMode.TRANSMISSION, ConfiguredFailoverProxyProvider.class, 6 * LOOPS,
        0 * LOOPS, false, 2);

    /*  TestCase 3.2
     *  If the NameserviceStateIdStateIdMode of client is PROXY,
     *  router will connect observe with state id managed by router.
     *  In this unit-test, we will do below steps:
     *   (1) fs1 create   Router will just connect active
     *   (2) fs1 read     Router will connect observer because state is update in step (1)
     *   (3) fs2 create   Router will just connect active
     *   (4) stop observer EditLogTailer.
     *   (5) fs1 read     Router will connect active. the state manged by router is updated in
     *                    step (3), but observer is far behind. So router will failover to active.
     *  active ops is 2 (fs1's create, complete) * LOOP
     *              + 1 (fs1's msync) * LOOP
     *              + 2 (fs2's create, complete) * LOOP
     *              + 2 (fs1's open + msync) * LOOP
     *  observer ops is 1 (fs1's open) * LOOP
     * */
    testObserveRead(NameServiceStateIdMode.PROXY, NameServiceStateIdMode.TRANSMISSION,
        ObserverReadProxyProvider.class, 7 * LOOPS, 1 * LOOPS, false, 2);

    /*  TestCase 3.3
     *  If the NameserviceStateIdStateIdMode of client is TRANSMISSION,
     *  router will connect observe with state id managed by client.
     *  In this unit-test, we will do below steps:
     *   (1) fs1 create   Router will just connect active
     *   (2) fs1 read     Router will connect observer because state is update in step (1)
     *   (3) fs2 create   Router will just connect active
     *   (4) stop observer EditLogTailer.
     *   (5) fs1 read     Router will connect observer. Though observer is far behind, fs use their
     *                    own state id, observer will not reject fs1.
     *  active ops is 2 (fs1's create, complete) * LOOP
     *              + 2 (fs2's create, complete) * LOOP
     *  observer ops is 1 (fs1's open) * LOOP
     *                + 1 (fs3's open) * LOOP
     * */
    testObserveRead(NameServiceStateIdMode.TRANSMISSION, NameServiceStateIdMode.TRANSMISSION,
        ObserverReadProxyProvider.class, 4 * LOOPS, 2 * LOOPS, false, 2);
  }

  /* TestCases for observer is down. */
  @Test(timeout = 120000)
  public void testObserveReadWithRouterWhenObserverIsDown() throws Exception {
    /* TestCase1:
     * If the state id is managed by client, router just connector active NameNode.
     *   active ops is 2 (fs1's create, complete) * LOOP
     *               + 1 (fs1's open) * LOOP
     *               + 2 (fs2's create, complete) * LOOP
     *               + 1 (fs1's open) * LOOP
     *  */
    testObserveRead(NameServiceStateIdMode.TRANSMISSION,
        NameServiceStateIdMode.TRANSMISSION, ObserverReadProxyProvider.class, 6 * LOOPS, 0 * LOOPS,
        true, 1);

    /* TestCase2:
     * If the state id is managed by router, in every read opeation, router will add a sync
     * operation.
     *   active ops is 2 (fs1's create, complete) * LOOP
     *               + 2 (fs1's open, msync) * LOOP
     *               + 2 (fs2's create, complete) * LOOP
     *               + 2 (fs1's open, msync) * LOOP
     *  */
    testObserveRead(NameServiceStateIdMode.PROXY, NameServiceStateIdMode.TRANSMISSION,
        ObserverReadProxyProvider.class, 8 * LOOPS, 0 * LOOPS, true, 1);
    testObserveRead(NameServiceStateIdMode.DISABLE, NameServiceStateIdMode.PROXY,
        ObserverReadProxyProvider.class, 8 * LOOPS, 0 * LOOPS, true, 1);
    testObserveRead(NameServiceStateIdMode.TRANSMISSION, NameServiceStateIdMode.PROXY,
        ObserverReadProxyProvider.class, 8 * LOOPS, 0 * LOOPS, true, 1);
    testObserveRead(NameServiceStateIdMode.PROXY, NameServiceStateIdMode.PROXY,
        ObserverReadProxyProvider.class, 8 * LOOPS, 0 * LOOPS, true, 1);
    testObserveRead(NameServiceStateIdMode.TRANSMISSION, NameServiceStateIdMode.PROXY,
        ConfiguredFailoverProxyProvider.class, 8 * LOOPS, 0 * LOOPS, true, 1);
    testObserveRead(NameServiceStateIdMode.PROXY, NameServiceStateIdMode.PROXY,
        ConfiguredFailoverProxyProvider.class, 8 * LOOPS, 0 * LOOPS, true, 1);
  }

  @Test(timeout = 60000)
  public void testUnavaliableObserverNN() throws Exception {
    startUpCluster(2);
    Configuration clientConf = createClientConf(NameServiceStateIdMode.DISABLE,
        ObserverReadProxyProvider.class);
    FileSystem fileSystem = cluster.getFileSystem(FEDERATION_NS, clientConf);

    stopObserver(2);

    fileSystem.listStatus(new Path("/"));

    // msync, getBlockLocation  call should send to active when observer is stoped.
    assertProxyOps(cluster, 2, -1);

    fileSystem.close();

    boolean hasUnavailable = false;
    for(String ns : cluster.getNameservices()) {
      for (RouterContext routerContext : cluster.getRouters()) {
        List<? extends FederationNamenodeContext> nns = routerContext.getRouter()
            .getNamenodeResolver().getNamenodesForNameserviceId(ns, false);
        for(FederationNamenodeContext nn : nns) {
          if(FederationNamenodeServiceState.UNAVAILABLE == nn.getState()) {
            hasUnavailable = true;
          }
        }
      }
    }
    // After communicate with unavailable observer namenode,
    // we will update state to unavailable.
    assertTrue("There must has unavailable NN", hasUnavailable);
  }

  @Test(timeout = 120000)
  public void testRouterReadWithMsync() throws Exception {
    /*
    * Case 1:
    *   Router' mode is PROXY or DISABLE, msync will be ignored.
    * */
    testObserveReadWithMsync(NameServiceStateIdMode.TRANSMISSION, NameServiceStateIdMode.PROXY,
        ObserverReadProxyProvider.class, 1 + 3 * LOOPS, 1 + 1 * LOOPS);
    testObserveReadWithMsync(NameServiceStateIdMode.TRANSMISSION, NameServiceStateIdMode.DISABLE,
        ObserverReadProxyProvider.class, 1 + 3 * LOOPS, 0 * LOOPS);

    /*
     * Case 2:
     *   Router is TRANSMISSION, msync will be called by their demand.
     * */
    testObserveReadWithMsync(NameServiceStateIdMode.TRANSMISSION,
        NameServiceStateIdMode.TRANSMISSION, ObserverReadProxyProvider.class,
        1 + 3 * LOOPS, 1 + 1 * LOOPS);
  }

  @Test(timeout = 600000)
  public void testObserveReadWithRouterAndContinuousMsync() throws Exception {
    // Here we simulate the whole pipline, include msync from namenode.
    // Because client will trigger msync and observer is far behind, so the second time of fs1
    // read will connect to active.
    startUpCluster(1, NameServiceStateIdMode.TRANSMISSION, null);
    Configuration clientConf = createClientConf(NameServiceStateIdMode.TRANSMISSION,
        ObserverReadProxyProvider.class, 0);
    testExamples(cluster, clientConf, -1, 1 * LOOPS, false);
  }

  private void testObserveReadWithMsync(NameServiceStateIdMode clientMode,
                                        NameServiceStateIdMode routerMode,
                                        Class<? extends FailoverProxyProvider> provider,
                                        int activeExpected,
                                        int observerExpected) throws Exception {
    try {
      startUpCluster(1, routerMode, null);
      Configuration clientConf = createClientConf(clientMode, provider);
      testExamplesWithMsync(cluster, clientConf, activeExpected, observerExpected);
      // assertEquals(0L, RouterStateIdCache.size());
    } finally {
      teardown();
    }
  }

  private void testObserveRead(NameServiceStateIdMode clientMode,
                               NameServiceStateIdMode routerMode,
                               Class<? extends FailoverProxyProvider> provider,
                               int activeExpected,
                               int observerExpected,
                               boolean stopObserver,
                               int observers) throws Exception {
    try {
      startUpCluster(observers, routerMode, null);
      Configuration clientConf = createClientConf(clientMode, provider);
      testExamples(cluster, clientConf, activeExpected, observerExpected, stopObserver);
      // assertEquals(0L, RouterStateIdCache.size());
    } finally {
      teardown();
    }
  }

  private Configuration createClientConf(NameServiceStateIdMode mode,
      Class<? extends FailoverProxyProvider> provider) {
    return createClientConf(mode, provider, -1);
  }

  private Configuration createClientConf(NameServiceStateIdMode mode,
        Class<? extends FailoverProxyProvider> provider, int period){
    Configuration clientConf = new Configuration();
    clientConf.set(PROXY_PROVIDER_KEY_PREFIX + "." + FEDERATION_NS, provider.getCanonicalName());
      clientConf.setInt(AUTO_MSYNC_PERIOD_KEY_PREFIX + "." + FEDERATION_NS, period);
    clientConf.set(HdfsClientConfigKeys.DFS_CLIENT_NAMESERVICE_STATE_ID_MODE, mode.name());
    clientConf.setLong(ObserverReadProxyProvider.OBSERVER_PROBE_RETRY_PERIOD_KEY, 0L);
    // For TRANSMISSION mode, different user should use their own AlignmentContext in test.
    clientConf.setBoolean("fs.hdfs.impl.disable.cache", true);
    return clientConf;
  }

  private void testExamplesWithMsync(MiniRouterDFSCluster cluster, Configuration clientConf,
      int activeExpected, int observerExpected) throws Exception {
    Path path = new Path("/testFile");
    String defaultNs = cluster.getDefaultNameservice();
    FileSystem fs1 = cluster.getFileSystem(FEDERATION_NS, clientConf);
    try {
      waitObserverKeepUpWithActive(cluster, defaultNs);
      // initializeMsync. mysnc to active only at first time, list to observer.
      // In PROXY mode, RouterClientProtocol::msync will be ignored. But RouterRpcClient::msync will
      // be called.
      // In TRANSMISSION, msync will be ignored firstly, then RouterRpcClient::msync found no nn was
      // connected, so trigger msync. Add one rpc to active.
      fs1.listStatus(new Path("/"));

      for (int i = 0; i < LOOPS; i ++) {
        // Send create and complete call to active
        fs1.create(path).close();

        waitObserverKeepUpWithActive(cluster, defaultNs);

        // Send sync.
        // In PROXY mode, RouterClientProtocol::msync will be ignored. But RouterRpcClient::msync
        // will be called.
        // In TRANSMISSION, msync will be called by demand.
        // Here fs1 only visit ns1's namenode, because we only have connected ns1.
        fs1.msync();

        // Send read request to observer. msync to active, open to observer
        fs1.open(path).close();
      }
    } finally {
      if (fs1 != null) {
        fs1.close();
      }
    }
    assertProxyOps(cluster, activeExpected, observerExpected);
  }

  private void testExamples(MiniRouterDFSCluster cluster, Configuration clientConf,
      int activeExpected, int observerExpected, boolean stopObserver) throws Exception {
    if (stopObserver) {
      int nnIndex = stopObserver(1);
      assertNotEquals("No observer found", 3, nnIndex);
    }
    String defaultNs = cluster.getDefaultNameservice();
    FileSystem fs1 = cluster.getFileSystem(FEDERATION_NS, clientConf);
    FileSystem fs2 = cluster.getFileSystem(FEDERATION_NS, clientConf);
    try {
      fs1 = cluster.getFileSystem(FEDERATION_NS, clientConf);
      fs2 = cluster.getFileSystem(FEDERATION_NS, clientConf);
      for (int i = 0; i < LOOPS; i++) {
        Path path1 = new Path("/testFile1" + i);
        // Send create call and compelete to active
        fs1.create(path1).close();

        if (!stopObserver) {
          // wait observer keep up with active
          waitObserverKeepUpWithActive(cluster, defaultNs);
        }

        // Send read request to observer
        fs1.open(path1).close();

        // stop observer namenode editlogtailer, obeserver's state id will delay
        stopObserverEditLogTailer(cluster, defaultNs);

        Path path2 = new Path("/testFile2" + i);
        // Send create call to active
        fs2.create(path2).close();

        // observer state id is delayed.
        fs1.open(path1).close();

        startObserverEditLogTailer(cluster, defaultNs);
      }
    } finally {
      if (fs1 != null) {
        fs1.close();
      }
      if (fs2 != null) {
        fs2.close();
      }
    }
    assertProxyOps(cluster, activeExpected, observerExpected);
  }

  private int stopObserver(int num) {
    int nnIndex;
    for (nnIndex = 0; nnIndex < cluster.getNamenodes().size(); nnIndex++) {
      NameNode nameNode = cluster.getCluster().getNameNode(nnIndex);
      if (nameNode != null && nameNode.isObserverState()) {
        cluster.getCluster().shutdownNameNode(nnIndex);
        num--;
        if (num == 0) {
          break;
        }
      }
    }
    return nnIndex;
  }

  public List<NameNode> getNameNodes(MiniRouterDFSCluster cluster,
                                     String nsId,
                                     HAServiceState state) {
    List<NameNode> nameNodes = new ArrayList<>();
    for (MiniRouterDFSCluster.NamenodeContext context :cluster.getNamenodes(nsId)) {
      if (context.getNamenode().getState().equals(state.toString())) {
        nameNodes.add(context.getNamenode());
      }
    }
    return nameNodes;
  }

  private void waitObserverKeepUpWithActive(MiniRouterDFSCluster cluster, String nsId)
      throws Exception {
    List<MiniRouterDFSCluster.NamenodeContext> namenodeContexts = cluster.getNamenodes(nsId);
    LambdaTestUtils.await(2000, 10, () -> {
      List<NameNode> actives = getNameNodes(cluster, nsId, HAServiceState.ACTIVE);
      List<NameNode> observers = getNameNodes(cluster, nsId, HAServiceState.OBSERVER);
      assertEquals("Active namenode must have one instance.", 1, actives.size());
      assertNotNull("Can't find observer namenode.", observers.size() > 0);
      return observers.stream()
          .map(o -> o.getNamesystem().getLastAppliedOrWrittenTxId() ==
              actives.get(0).getNamesystem().getLastAppliedOrWrittenTxId())
          .reduce(true, (a, b) -> a && b);
    });
  }

  private void assertProxyOps(MiniRouterDFSCluster cluster, int active, int observer) {
    long rpcCountForActive = cluster.getRouters().stream()
        .map(s -> s.getRouter().getRpcServer().getRPCMetrics().getActiveProxyOps())
        .reduce(0L, Long::sum);
    long rpcCountForObserver = cluster.getRouters().stream()
        .map(s -> s.getRouter().getRpcServer().getRPCMetrics().getObserverProxyOps())
        .reduce(0L, Long::sum);
    if (active >= 0) {
      assertEquals(active, rpcCountForActive);
    }
    if (observer >= 0) {
      assertEquals(observer, rpcCountForObserver);
    }
  }

  private void stopObserverEditLogTailer(MiniRouterDFSCluster cluster, String defaultNs)
      throws InterruptedException {
    List<NameNode> observers = getNameNodes(cluster, defaultNs, HAServiceState.OBSERVER);
    observers.forEach(o -> o.getNamesystem().getEditLogTailer().setSkipForTest(true));
    Thread.sleep(20);
  }

  private void startObserverEditLogTailer(MiniRouterDFSCluster cluster, String defaultNs)
      throws InterruptedException {
    List<NameNode> observers = getNameNodes(cluster, defaultNs, HAServiceState.OBSERVER);
    observers.forEach(o -> o.getNamesystem().getEditLogTailer().setSkipForTest(false));
    Thread.sleep(20);
  }
}