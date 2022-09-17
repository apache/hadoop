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
package org.apache.hadoop.hdfs.server.federation.fairness;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the Router handlers fairness control rejects and accepts requests.
 */
public class TestRouterHandlersFairness {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterHandlersFairness.class);

  private StateStoreDFSCluster cluster;

  @After
  public void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private void setupCluster(boolean fairnessEnable, boolean ha)
      throws Exception {
    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(ha, 2);
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .rpc()
        .build();

    // Fairness control
    if (fairnessEnable) {
      routerConf.setClass(
          RBFConfigKeys.DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS,
          StaticRouterRpcFairnessPolicyController.class,
          RouterRpcFairnessPolicyController.class);
    }

    // With two name services configured, each nameservice has 1 permit and
    // fan-out calls have 1 permit.
    routerConf.setInt(RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY, 3);

    // Datanodes not needed for this test.
    cluster.setNumDatanodesPerNameservice(0);

    cluster.addRouterOverrides(routerConf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();
  }

  @Test
  public void testFairnessControlOff() throws Exception {
    setupCluster(false, false);
    startLoadTest(false);
  }

  @Test
  public void testFairnessControlOn() throws Exception {
    setupCluster(true, false);
    startLoadTest(true);
  }

  /**
   * Start a generic load test as a client against a cluster which has either
   * fairness configured or not configured. Test will spawn a set of 100
   * threads to simulate concurrent request to test routers. If fairness is
   * enabled, the test will successfully report the failure of some threads
   * to continue with StandbyException. If fairness is not configured, all
   * threads of the same test should successfully complete all the rpcs.
   *
   * @param fairness Flag to indicate if fairness management is on/off.
   * @throws Exception Throws exception.
   */
  private void startLoadTest(boolean fairness)
      throws Exception {

    // Concurrent requests
    startLoadTest(true, fairness);

    // Sequential requests
    startLoadTest(false, fairness);
  }

  private void startLoadTest(final boolean isConcurrent, final boolean fairness)
      throws Exception {

    RouterContext routerContext = cluster.getRandomRouter();
    URI address = routerContext.getFileSystemURI();
    Configuration conf = new HdfsConfiguration();
    final int numOps = 10;
    AtomicInteger overloadException = new AtomicInteger();

    // Test when handlers are overloaded
    if (fairness) {
      if (isConcurrent) {
        LOG.info("Taking fanout lock first");
        // take the lock for concurrent NS to block fanout calls
        assertTrue(routerContext.getRouter().getRpcServer()
            .getRPCClient().getRouterRpcFairnessPolicyController()
            .acquirePermit(RouterRpcFairnessConstants.CONCURRENT_NS));
      } else {
        for (String ns : cluster.getNameservices()) {
          LOG.info("Taking lock first for ns: {}", ns);
          assertTrue(routerContext.getRouter().getRpcServer()
              .getRPCClient().getRouterRpcFairnessPolicyController()
              .acquirePermit(ns));
        }
      }
    }
    int originalRejectedPermits = getTotalRejectedPermits(routerContext);

    // |- All calls should fail since permits not released
    innerCalls(address, numOps, isConcurrent, conf, overloadException);

    int latestRejectedPermits = getTotalRejectedPermits(routerContext);
    assertEquals(latestRejectedPermits - originalRejectedPermits,
        overloadException.get());

    if (fairness) {
      assertTrue(overloadException.get() > 0);
      if (isConcurrent) {
        LOG.info("Release fanout lock that was taken before test");
        // take the lock for concurrent NS to block fanout calls
        routerContext.getRouter().getRpcServer()
            .getRPCClient().getRouterRpcFairnessPolicyController()
            .releasePermit(RouterRpcFairnessConstants.CONCURRENT_NS);
      } else {
        for (String ns : cluster.getNameservices()) {
          routerContext.getRouter().getRpcServer()
              .getRPCClient().getRouterRpcFairnessPolicyController()
              .releasePermit(ns);
        }
      }
    } else {
      assertEquals("Number of failed RPCs without fairness configured",
          0, overloadException.get());
    }

    // Test when handlers are not overloaded
    int originalAcceptedPermits = getTotalAcceptedPermits(routerContext);
    overloadException = new AtomicInteger();

    // |- All calls should succeed since permits not acquired
    innerCalls(address, numOps, isConcurrent, conf, overloadException);

    int latestAcceptedPermits = getTotalAcceptedPermits(routerContext);
    assertEquals(latestAcceptedPermits - originalAcceptedPermits, numOps);
    assertEquals(overloadException.get(), 0);
  }

  private void invokeSequential(ClientProtocol routerProto) throws IOException {
    routerProto.getFileInfo("/test.txt");
  }

  private void invokeConcurrent(ClientProtocol routerProto, String clientName)
      throws IOException {
    routerProto.renewLease(clientName, null);
  }

  private int getTotalRejectedPermits(RouterContext routerContext) {
    int totalRejectedPermits = 0;
    for (String ns : cluster.getNameservices()) {
      totalRejectedPermits += routerContext.getRouterRpcClient()
          .getRejectedPermitForNs(ns);
    }
    totalRejectedPermits += routerContext.getRouterRpcClient()
        .getRejectedPermitForNs(RouterRpcFairnessConstants.CONCURRENT_NS);
    return totalRejectedPermits;
  }

  private int getTotalAcceptedPermits(RouterContext routerContext) {
    int totalAcceptedPermits = 0;
    for (String ns : cluster.getNameservices()) {
      totalAcceptedPermits += routerContext.getRouterRpcClient()
          .getAcceptedPermitForNs(ns);
    }
    totalAcceptedPermits += routerContext.getRouterRpcClient()
        .getAcceptedPermitForNs(RouterRpcFairnessConstants.CONCURRENT_NS);
    return totalAcceptedPermits;
  }

  private void innerCalls(URI address, int numOps, boolean isConcurrent,
      Configuration conf, AtomicInteger overloadException) throws IOException {
    for (int i = 0; i < numOps; i++) {
      DFSClient routerClient = null;
      try {
        routerClient = new DFSClient(address, conf);
        String clientName = routerClient.getClientName();
        ClientProtocol routerProto = routerClient.getNamenode();
        if (isConcurrent) {
          invokeConcurrent(routerProto, clientName);
        } else {
          invokeSequential(routerProto);
        }
      } catch (RemoteException re) {
        IOException ioe = re.unwrapRemoteException();
        assertTrue("Wrong exception: " + ioe,
            ioe instanceof StandbyException);
        assertExceptionContains("is overloaded for NS", ioe);
        overloadException.incrementAndGet();
      } catch (Throwable e) {
        throw e;
      } finally {
        if (routerClient != null) {
          try {
            routerClient.close();
          } catch (IOException e) {
            LOG.error("Cannot close the client");
          }
        }
      }
      overloadException.get();
    }
  }
}
