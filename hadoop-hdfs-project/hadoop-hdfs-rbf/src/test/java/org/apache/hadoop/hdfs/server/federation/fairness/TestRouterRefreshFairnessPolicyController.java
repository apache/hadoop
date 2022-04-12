/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.federation.fairness;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.RemoteMethod;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcClient;

import static org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessConstants.CONCURRENT_NS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestRouterRefreshFairnessPolicyController {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterRefreshFairnessPolicyController.class);

  private StateStoreDFSCluster cluster;

  @After
  public void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Before
  public void setupCluster() throws Exception {
    cluster = new StateStoreDFSCluster(false, 1);
    Configuration conf = new RouterConfigBuilder().stateStore().rpc().build();

    // Handlers concurrent:ns0 = 3:3
    conf.setClass(RBFConfigKeys.DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS,
        StaticRouterRpcFairnessPolicyController.class, RouterRpcFairnessPolicyController.class);
    conf.setInt(RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY, 6);

    // Datanodes not needed for this test.
    cluster.setNumDatanodesPerNameservice(0);

    cluster.addRouterOverrides(conf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();
  }

  @Test
  public void testRefreshStaticChangeHandlers() throws Exception {
    MiniRouterDFSCluster.RouterContext routerContext = cluster.getRandomRouter();
    RemoteMethod dummyMethod = Mockito.mock(RemoteMethod.class);
    RouterRpcClient client = Mockito.spy(routerContext.getRouterRpcClient());
    final long sleepTime = 3000;
    Mockito.doAnswer(invocationOnMock -> {
      Thread.sleep(sleepTime);
      return null;
    }).when(client)
        .invokeMethod(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

    final int N = 3;
    Thread[] threadAcquirePermits = new Thread[N];
    for (int i = 0; i < N; i++) {
      Thread threadAcquirePermit = new Thread(() -> {
        try {
          client.invokeSingle("ns0", dummyMethod);
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
      threadAcquirePermits[i] = threadAcquirePermit;
      threadAcquirePermits[i].start();
    }

    Thread.sleep(1000);

    Configuration conf = routerContext.getConf();
    final int newNs0Permits = 1; // Set to smaller than current handler count (3)
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "ns0", newNs0Permits);
    Thread threadRefreshController = new Thread(() -> {
      client.refreshFairnessPolicyController(routerContext.getConf());
    });
    threadRefreshController.start();
    threadRefreshController.join();

    StaticRouterRpcFairnessPolicyController controller =
        (StaticRouterRpcFairnessPolicyController) client.getRouterRpcFairnessPolicyController();
    for (int i = 0; i < N; i++) {
      threadAcquirePermits[i].join();
    }

    // Controller should now have 5:1 handlers for concurrent:ns0
    for (int i = 0; i < 5; i++) {
      assertTrue(controller.acquirePermit(CONCURRENT_NS));
    }
    // Invocations before refresh should not interfere with invocations after
    assertTrue(controller.acquirePermit("ns0"));

    // Acquiring a permit on any ns now will fail due to overload
    assertFalse(controller.acquirePermit(CONCURRENT_NS));
    assertFalse(controller.acquirePermit("ns0"));
  }
}
