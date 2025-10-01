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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.router.FederationUtil;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessConstants.CONCURRENT_NS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_MAX_ASYNCCALL_PERMIT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_MAX_ASYNC_CALL_PERMIT_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIRNESS_ACQUIRE_TIMEOUT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test functionality of {@link RouterAsyncRpcFairnessPolicyController).
 */
public class TestRouterAsyncRpcFairnessPolicyController {

  private static String nameServices =
      "ns1.nn1, ns1.nn2, ns2.nn1, ns2.nn2";
  private static int perNsPermits = 30;

  @Test
  public void testHandlerAllocationEqualAssignment() {
    RouterRpcFairnessPolicyController routerRpcFairnessPolicyController
        = getFairnessPolicyController(perNsPermits);
    verifyHandlerAllocation(routerRpcFairnessPolicyController);
  }

  @Test
  public void testAcquireTimeout() {
    Configuration conf = createConf(perNsPermits);
    conf.setTimeDuration(DFS_ROUTER_FAIRNESS_ACQUIRE_TIMEOUT, 100, TimeUnit.MILLISECONDS);
    RouterRpcFairnessPolicyController routerRpcFairnessPolicyController =
        FederationUtil.newFairnessPolicyController(conf);

    // Ns1 should have number of perNsPermits permits allocated.
    for (int i = 0; i < perNsPermits; i++) {
      assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns1"));
    }
    long acquireBeginTimeMs = Time.monotonicNow();
    assertFalse(routerRpcFairnessPolicyController.acquirePermit("ns1"));
    long acquireTimeMs = Time.monotonicNow() - acquireBeginTimeMs;

    // There are some other operations, so acquireTimeMs >= 100ms.
    assertTrue(acquireTimeMs >= 100);
  }

  @Test
  public void testAllocationSuccessfullyWithZeroHandlers() {
    Configuration conf = createConf(0);
    verifyInstantiationStatus(conf, DFS_ROUTER_ASYNC_RPC_MAX_ASYNC_CALL_PERMIT_DEFAULT);
  }

  @Test
  public void testAllocationSuccessfullyWithNegativePermits() {
    Configuration conf = createConf(-1);
    verifyInstantiationStatus(conf, DFS_ROUTER_ASYNC_RPC_MAX_ASYNC_CALL_PERMIT_DEFAULT);
  }

  @Test
  public void testGetAvailableHandlerOnPerNs() {
    RouterRpcFairnessPolicyController routerRpcFairnessPolicyController
        = getFairnessPolicyController(perNsPermits);
    assertEquals("{\"concurrent\":30,\"ns2\":30,\"ns1\":30}",
        routerRpcFairnessPolicyController.getAvailableHandlerOnPerNs());
    routerRpcFairnessPolicyController.acquirePermit("ns1");
    assertEquals("{\"concurrent\":30,\"ns2\":30,\"ns1\":29}",
        routerRpcFairnessPolicyController.getAvailableHandlerOnPerNs());
  }

  @Test
  public void testGetAvailableHandlerOnPerNsForNoFairness() {
    Configuration conf = new Configuration();
    RouterRpcFairnessPolicyController routerRpcFairnessPolicyController =
        FederationUtil.newFairnessPolicyController(conf);
    assertEquals("N/A",
        routerRpcFairnessPolicyController.getAvailableHandlerOnPerNs());
  }

  private void verifyInstantiationStatus(Configuration conf, int permits) {
    GenericTestUtils.LogCapturer logs = GenericTestUtils.LogCapturer
        .captureLogs(LoggerFactory.getLogger(
            RouterAsyncRpcFairnessPolicyController.class));
    try {
      FederationUtil.newFairnessPolicyController(conf);
    } catch (IllegalArgumentException e) {
      // Ignore the exception as it is expected here.
    }
    String infoMsg = String.format(
        RouterAsyncRpcFairnessPolicyController.INIT_MSG, permits);
    assertTrue(logs.getOutput().contains(infoMsg), "Should contain info message: " +
        infoMsg);
  }

  private RouterRpcFairnessPolicyController getFairnessPolicyController(
      int asyncCallPermits) {
    return FederationUtil.newFairnessPolicyController(createConf(asyncCallPermits));
  }

  private void verifyHandlerAllocation(
      RouterRpcFairnessPolicyController routerRpcFairnessPolicyController) {
    for (int i = 0; i < perNsPermits; i++) {
      assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns1"));
      assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns2"));
      // CONCURRENT_NS doesn't acquire permits.
      assertTrue(
          routerRpcFairnessPolicyController.acquirePermit(CONCURRENT_NS));
    }
    assertFalse(routerRpcFairnessPolicyController.acquirePermit("ns1"));
    assertFalse(routerRpcFairnessPolicyController.acquirePermit("ns2"));
    assertTrue(routerRpcFairnessPolicyController.acquirePermit(CONCURRENT_NS));

    routerRpcFairnessPolicyController.releasePermit("ns1");
    routerRpcFairnessPolicyController.releasePermit("ns2");
    routerRpcFairnessPolicyController.releasePermit(CONCURRENT_NS);

    assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns1"));
    assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns2"));
    assertTrue(routerRpcFairnessPolicyController.acquirePermit(CONCURRENT_NS));
  }

  private Configuration createConf(int asyncCallPermits) {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFS_ROUTER_ASYNC_RPC_MAX_ASYNCCALL_PERMIT_KEY, asyncCallPermits);
    conf.set(DFS_ROUTER_MONITOR_NAMENODE, nameServices);
    conf.setClass(
        RBFConfigKeys.DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS,
        RouterAsyncRpcFairnessPolicyController.class,
        RouterRpcFairnessPolicyController.class);
    return conf;
  }
}
