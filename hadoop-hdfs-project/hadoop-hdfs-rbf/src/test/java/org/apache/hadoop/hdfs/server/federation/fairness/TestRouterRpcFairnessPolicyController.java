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
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessConstants.CONCURRENT_NS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test functionality of {@link RouterRpcFairnessPolicyController).
 */
public class TestRouterRpcFairnessPolicyController {

  private static String nameServices =
      "ns1.nn1, ns1.nn2, ns2.nn1, ns2.nn2";

  @Test
  public void testHandlerAllocationEqualAssignment() {
    RouterRpcFairnessPolicyController routerRpcFairnessPolicyController
        = getFairnessPolicyController(30);
    verifyHandlerAllocation(routerRpcFairnessPolicyController);
  }

  @Test
  public void testHandlerAllocationWithLeftOverHandler() {
    RouterRpcFairnessPolicyController routerRpcFairnessPolicyController
        = getFairnessPolicyController(31);
    // One extra handler should be allocated to commons.
    assertTrue(routerRpcFairnessPolicyController.acquirePermit(CONCURRENT_NS)
        .isHoldPermit());
    verifyHandlerAllocation(routerRpcFairnessPolicyController);
  }

  @Test
  public void testHandlerAllocationPreconfigured() {
    Configuration conf = createConf(40);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "ns1", 30);
    RouterRpcFairnessPolicyController routerRpcFairnessPolicyController =
        FederationUtil.newFairnessPolicyController(conf, 0);

    // ns1 should have 30 permits allocated
    for (int i=0; i<30; i++) {
      assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns1")
          .isHoldPermit());
    }

    // ns2 should have 5 permits.
    // concurrent should have 5 permits.
    for (int i=0; i<5; i++) {
      assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns2")
          .isHoldPermit());
      assertTrue(
          routerRpcFairnessPolicyController.acquirePermit(CONCURRENT_NS)
              .isHoldPermit());
    }

    assertFalse(routerRpcFairnessPolicyController.acquirePermit("ns1")
        .isHoldPermit());
    assertFalse(routerRpcFairnessPolicyController.acquirePermit("ns2")
        .isHoldPermit());
    assertFalse(routerRpcFairnessPolicyController.acquirePermit(CONCURRENT_NS)
        .isHoldPermit());
  }

  @Test
  public void testAllocationErrorWithZeroHandlers() {
    Configuration conf = createConf(0);
    verifyInstantiationError(conf, 0, 3);
  }

  @Test
  public void testAllocationErrorForLowDefaultHandlers() {
    Configuration conf = createConf(1);
    verifyInstantiationError(conf, 1, 3);
  }

  @Test
  public void testAllocationErrorForLowDefaultHandlersPerNS() {
    Configuration conf = createConf(1);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "concurrent", 1);
    verifyInstantiationError(conf, 1, 3);
  }

  @Test
  public void testGetAvailableHandlerOnPerNs() {
    RouterRpcFairnessPolicyController routerRpcFairnessPolicyController
        = getFairnessPolicyController(30);
    assertEquals("{\"concurrent\":10,\"ns2\":10,\"ns1\":10}",
        routerRpcFairnessPolicyController.getAvailableHandlerOnPerNs());
    routerRpcFairnessPolicyController.acquirePermit("ns1");
    assertEquals("{\"concurrent\":10,\"ns2\":10,\"ns1\":9}",
        routerRpcFairnessPolicyController.getAvailableHandlerOnPerNs());
  }

  @Test
  public void testGetAvailableHandlerOnPerNsForNoFairness() {
    Configuration conf = new Configuration();
    RouterRpcFairnessPolicyController routerRpcFairnessPolicyController =
        FederationUtil.newFairnessPolicyController(conf, 0);
    assertEquals("N/A",
        routerRpcFairnessPolicyController.getAvailableHandlerOnPerNs());
  }

  @Test
  public void testAllocationErrorForLowPreconfiguredHandlers() {
    Configuration conf = createConf(1);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "ns1", 2);
    verifyInstantiationError(conf, 1, 4);
  }

  @Test
  public void testHandlerAllocationConcurrentConfigured() {
    Configuration conf = createConf(5);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "ns1", 1);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "ns2", 1);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "concurrent", 1);
    RouterRpcFairnessPolicyController routerRpcFairnessPolicyController =
        FederationUtil.newFairnessPolicyController(conf, 0);

    // ns1, ns2 should have 1 permit each
    assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns1")
        .isHoldPermit());
    assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns2")
        .isHoldPermit());
    assertFalse(routerRpcFairnessPolicyController.acquirePermit("ns1")
        .isHoldPermit());
    assertFalse(routerRpcFairnessPolicyController.acquirePermit("ns2")
        .isHoldPermit());

    // concurrent should have 3 permits
    for (int i=0; i<3; i++) {
      assertTrue(
          routerRpcFairnessPolicyController.acquirePermit(CONCURRENT_NS)
              .isHoldPermit());
    }
    assertFalse(routerRpcFairnessPolicyController.acquirePermit(CONCURRENT_NS)
        .isHoldPermit());
  }


  private void verifyInstantiationError(Configuration conf, int handlerCount,
      int totalDedicatedHandlers) {
    GenericTestUtils.LogCapturer logs = GenericTestUtils.LogCapturer
        .captureLogs(LoggerFactory.getLogger(
            AbstractRouterRpcFairnessPolicyController.class));
    try {
      FederationUtil.newFairnessPolicyController(conf, 0);
    } catch (IllegalArgumentException e) {
      // Ignore the exception as it is expected here.
    }
    String errorMsg = String.format(
        AbstractRouterRpcFairnessPolicyController.ERROR_MSG, handlerCount,
        totalDedicatedHandlers);
    assertTrue("Should contain error message: " + errorMsg,
        logs.getOutput().contains(errorMsg));
  }

  private RouterRpcFairnessPolicyController getFairnessPolicyController(
      int handlers) {
    return FederationUtil.newFairnessPolicyController(createConf(handlers), 0);
  }

  private void verifyHandlerAllocation(
      RouterRpcFairnessPolicyController routerRpcFairnessPolicyController) {
    for (int i=0; i<10; i++) {
      assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns1")
          .isHoldPermit());
      assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns2")
          .isHoldPermit());
      assertTrue(
          routerRpcFairnessPolicyController.acquirePermit(CONCURRENT_NS)
              .isHoldPermit());
    }
    assertFalse(routerRpcFairnessPolicyController.acquirePermit("ns1")
        .isHoldPermit());
    assertFalse(routerRpcFairnessPolicyController.acquirePermit("ns2")
        .isHoldPermit());
    assertFalse(routerRpcFairnessPolicyController.acquirePermit(CONCURRENT_NS)
        .isHoldPermit());

    Permit permit = new Permit(Permit.PermitType.DEDICATED,
        routerRpcFairnessPolicyController.getVersion());

    routerRpcFairnessPolicyController.releasePermit("ns1", permit);
    routerRpcFairnessPolicyController.releasePermit("ns2", permit);
    routerRpcFairnessPolicyController.releasePermit(CONCURRENT_NS, permit);

    assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns1")
        .isHoldPermit());
    assertTrue(routerRpcFairnessPolicyController.acquirePermit("ns2")
        .isHoldPermit());
    assertTrue(routerRpcFairnessPolicyController.acquirePermit(CONCURRENT_NS)
        .isHoldPermit());
  }

  private Configuration createConf(int handlers) {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFS_ROUTER_HANDLER_COUNT_KEY, handlers);
    conf.set(DFS_ROUTER_MONITOR_NAMENODE, nameServices);
    conf.setClass(
        RBFConfigKeys.DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS,
        StaticRouterRpcFairnessPolicyController.class,
        RouterRpcFairnessPolicyController.class);
    return conf;
  }
}
