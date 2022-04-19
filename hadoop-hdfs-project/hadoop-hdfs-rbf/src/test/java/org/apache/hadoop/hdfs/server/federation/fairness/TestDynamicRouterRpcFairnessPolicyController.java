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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;

import static org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessConstants.CONCURRENT_NS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE;

/**
 * Test functionality of {@link DynamicRouterRpcFairnessPolicyController).
 */
public class TestDynamicRouterRpcFairnessPolicyController {

  private static String nameServices = "ns1.nn1, ns1.nn2, ns2.nn1, ns2.nn2";

  @Test
  public void testDynamicControllerSimple() throws InterruptedException {
    verifyDynamicControllerSimple(true);
    verifyDynamicControllerSimple(false);
  }

  @Test
  public void testDynamicControllerAllPermitsAcquired() throws InterruptedException {
    verifyDynamicControllerAllPermitsAcquired(true);
    verifyDynamicControllerAllPermitsAcquired(false);
  }

  private void verifyDynamicControllerSimple(boolean manualRefresh)
      throws InterruptedException {
    // 3 permits each ns
    DynamicRouterRpcFairnessPolicyController controller;
    if (manualRefresh) {
      controller = getFairnessPolicyController(9);
    } else {
      controller = getFairnessPolicyController(9, 4000);
    }
    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(controller.acquirePermit("ns1"));
      Assert.assertTrue(controller.acquirePermit("ns2"));
      Assert.assertTrue(controller.acquirePermit(CONCURRENT_NS));
    }
    Assert.assertFalse(controller.acquirePermit("ns1"));
    Assert.assertFalse(controller.acquirePermit("ns2"));
    Assert.assertFalse(controller.acquirePermit(CONCURRENT_NS));

    // Release all permits
    for (int i = 0; i < 3; i++) {
      controller.releasePermit("ns1");
      controller.releasePermit("ns2");
      controller.releasePermit(CONCURRENT_NS);
    }

    // Inject dummy metrics
    // Split half half for ns1 and concurrent
    Map<String, LongAdder> rejectedPermitsPerNs = new HashMap<>();
    Map<String, LongAdder> acceptedPermitsPerNs = new HashMap<>();
    injectDummyMetrics(rejectedPermitsPerNs, "ns1", 10);
    injectDummyMetrics(rejectedPermitsPerNs, "ns2", 0);
    injectDummyMetrics(rejectedPermitsPerNs, CONCURRENT_NS, 10);
    controller.setMetrics(rejectedPermitsPerNs, acceptedPermitsPerNs);

    if (manualRefresh) {
      controller.refreshPermitsCap();
    } else {
      Thread.sleep(5000);
    }

    // Current permits count should be 5:1:5
    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(controller.acquirePermit("ns1"));
      Assert.assertTrue(controller.acquirePermit(CONCURRENT_NS));
    }
    Assert.assertTrue(controller.acquirePermit("ns2"));

    Assert.assertFalse(controller.acquirePermit("ns1"));
    Assert.assertFalse(controller.acquirePermit("ns2"));
    Assert.assertFalse(controller.acquirePermit(CONCURRENT_NS));
  }

  public void verifyDynamicControllerAllPermitsAcquired(boolean manualRefresh)
      throws InterruptedException {
    // 10 permits each ns
    DynamicRouterRpcFairnessPolicyController controller;
    if (manualRefresh) {
      controller = getFairnessPolicyController(30);
    } else {
      controller = getFairnessPolicyController(30, 4000);
    }
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(controller.acquirePermit("ns1"));
      Assert.assertTrue(controller.acquirePermit("ns2"));
      Assert.assertTrue(controller.acquirePermit(CONCURRENT_NS));
    }

    // Inject dummy metrics
    Map<String, LongAdder> rejectedPermitsPerNs = new HashMap<>();
    Map<String, LongAdder> acceptedPermitsPerNs = new HashMap<>();
    injectDummyMetrics(rejectedPermitsPerNs, "ns1", 14);
    injectDummyMetrics(rejectedPermitsPerNs, "ns2", 14);
    injectDummyMetrics(rejectedPermitsPerNs, CONCURRENT_NS, 2);
    controller.setMetrics(rejectedPermitsPerNs, acceptedPermitsPerNs);
    if (manualRefresh) {
      controller.refreshPermitsCap();
    } else {
      Thread.sleep(5000);
    }
    Assert.assertEquals("{\"concurrent\":-8,\"ns2\":4,\"ns1\":4}",
        controller.getAvailableHandlerOnPerNs());

    // Current permits count should be 14:14:2
    // Can acquire 4 more permits for ns1 and ns2
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(controller.acquirePermit("ns1"));
      Assert.assertTrue(controller.acquirePermit("ns2"));
    }
    Assert.assertFalse(controller.acquirePermit("ns1"));
    Assert.assertFalse(controller.acquirePermit("ns2"));
    // Need to release at least 9 permits for concurrent before it has any free permits
    Assert.assertFalse(controller.acquirePermit(CONCURRENT_NS));
    for (int i = 0; i < 8; i++) {
      controller.releasePermit(CONCURRENT_NS);
    }
    Assert.assertFalse(controller.acquirePermit(CONCURRENT_NS));
    controller.releasePermit(CONCURRENT_NS);
    Assert.assertTrue(controller.acquirePermit(CONCURRENT_NS));
  }


  private void injectDummyMetrics(Map<String, LongAdder> metrics, String ns, long value) {
    metrics.computeIfAbsent(ns, k -> new LongAdder()).add(value);
  }

  private DynamicRouterRpcFairnessPolicyController getFairnessPolicyController(int handlers,
      long refreshInterval) {
    return new DynamicRouterRpcFairnessPolicyController(createConf(handlers), refreshInterval);
  }

  private DynamicRouterRpcFairnessPolicyController getFairnessPolicyController(int handlers) {
    return new DynamicRouterRpcFairnessPolicyController(createConf(handlers), Long.MAX_VALUE);
  }

  private Configuration createConf(int handlers) {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFS_ROUTER_HANDLER_COUNT_KEY, handlers);
    conf.set(DFS_ROUTER_MONITOR_NAMENODE, nameServices);
    conf.setClass(RBFConfigKeys.DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS,
        DynamicRouterRpcFairnessPolicyController.class, RouterRpcFairnessPolicyController.class);
    return conf;
  }
}
