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
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.junit.Test;

import static org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessConstants.CONCURRENT_NS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ELASTIC_PERMITS_PERCENT_DEFAULT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ELASTIC_PERMITS_PERCENT_KEY_PREFIX;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.hdfs.server.federation.fairness.Permit.PermitType;

/**
 * Test functionality of {@link ElasticRouterRpcFairnessPolicyController).
 */
public class TestElasticRouterRpcFairnessPolicyController {
  private final static String NAMESERVICES =
      "ns1.nn1, ns1.nn2, ns2.nn1, ns2.nn2";

  private void verifyAcquirePermit(RouterRpcFairnessPolicyController controller,
      String nsId, int dedicatedPermit, int elasticPermit) {
    for (int i = 0; i < dedicatedPermit; i++) {
      assertEquals(controller.acquirePermit(nsId)
          .getPermitType(), PermitType.DEDICATED);
    }
    for (int i = 0; i < elasticPermit; i++) {
      assertEquals(controller.acquirePermit(nsId)
          .getPermitType(), PermitType.SHARED);
    }
    assertEquals(controller.acquirePermit(nsId)
        .getPermitType(), PermitType.NO_PERMIT);
  }

  @Test
  public void testNoElasticPermits() {
    Configuration conf = createConf(10);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "ns1", 5);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "ns2", 2);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + CONCURRENT_NS, 3);

    ElasticRouterRpcFairnessPolicyController controller =
        new ElasticRouterRpcFairnessPolicyController(conf, 0);
    assertNotNull(controller);
    assertEquals(0, controller.getTotalElasticAvailableHandler());

    verifyAcquirePermit(controller, "ns1", 5, 0);
    verifyAcquirePermit(controller, "ns2", 2, 0);
    verifyAcquirePermit(controller, CONCURRENT_NS, 3, 0);
  }

  @Test
  public void testConfNoUseElasticPermits() {
    Configuration conf = createConf(20);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "ns1", 5);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "ns2", 2);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + CONCURRENT_NS, 3);
    conf.setInt(DFS_ROUTER_ELASTIC_PERMITS_PERCENT_DEFAULT_KEY, 0);

    ElasticRouterRpcFairnessPolicyController controller =
        new ElasticRouterRpcFairnessPolicyController(conf, 0);
    assertNotNull(controller);
    assertEquals(10, controller.getTotalElasticAvailableHandler());

    verifyAcquirePermit(controller, "ns1", 5, 0);
    verifyAcquirePermit(controller, "ns2", 2, 0);
    verifyAcquirePermit(controller, CONCURRENT_NS, 3, 0);
  }

  @Test
  public void testConfSmallerElasticPermits() {
    Configuration conf = createConf(20);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "ns1", 5);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "ns2", 2);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + CONCURRENT_NS, 3);
    conf.setInt(DFS_ROUTER_ELASTIC_PERMITS_PERCENT_KEY_PREFIX + "ns1", 20);
    conf.setInt(DFS_ROUTER_ELASTIC_PERMITS_PERCENT_KEY_PREFIX + "ns2", 20);
    conf.setInt(DFS_ROUTER_ELASTIC_PERMITS_PERCENT_KEY_PREFIX + CONCURRENT_NS, 20);

    ElasticRouterRpcFairnessPolicyController controller =
        new ElasticRouterRpcFairnessPolicyController(conf, 0);
    assertNotNull(controller);
    assertEquals(10, controller.getTotalElasticAvailableHandler());

    verifyAcquirePermit(controller, "ns1", 5, 2);
    verifyAcquirePermit(controller, "ns2", 2, 2);
    verifyAcquirePermit(controller, CONCURRENT_NS, 3, 2);
    assertEquals(4, controller.getTotalElasticAvailableHandler());
  }

  @Test
  public void testConfMoreElasticPermits() {
    Configuration conf = createConf(20);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "ns1", 5);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "ns2", 2);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + CONCURRENT_NS, 3);
    conf.setInt(DFS_ROUTER_ELASTIC_PERMITS_PERCENT_KEY_PREFIX + "ns1", 40);
    conf.setInt(DFS_ROUTER_ELASTIC_PERMITS_PERCENT_KEY_PREFIX + "ns2", 40);
    conf.setInt(DFS_ROUTER_ELASTIC_PERMITS_PERCENT_KEY_PREFIX + CONCURRENT_NS, 40);

    ElasticRouterRpcFairnessPolicyController controller =
        new ElasticRouterRpcFairnessPolicyController(conf, 0);
    assertNotNull(controller);
    assertEquals(10, controller.getTotalElasticAvailableHandler());

    verifyAcquirePermit(controller, "ns1", 5, 4);
    verifyAcquirePermit(controller, "ns2", 2, 4);
    verifyAcquirePermit(controller, CONCURRENT_NS, 3, 2);
    assertEquals(0, controller.getTotalElasticAvailableHandler());
  }

  @Test
  public void testConfMoreElasticPermits2() {
    Configuration conf = createConf(20);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "ns1", 5);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "ns2", 2);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + CONCURRENT_NS, 3);
    conf.setInt(DFS_ROUTER_ELASTIC_PERMITS_PERCENT_KEY_PREFIX + "ns1", 100);
    conf.setInt(DFS_ROUTER_ELASTIC_PERMITS_PERCENT_KEY_PREFIX + "ns2", 100);
    conf.setInt(DFS_ROUTER_ELASTIC_PERMITS_PERCENT_KEY_PREFIX + CONCURRENT_NS, 100);

    ElasticRouterRpcFairnessPolicyController controller =
        new ElasticRouterRpcFairnessPolicyController(conf, 0);
    assertNotNull(controller);
    assertEquals(10, controller.getTotalElasticAvailableHandler());

    verifyAcquirePermit(controller, "ns1", 5, 10);
    verifyAcquirePermit(controller, "ns2", 2, 0);
    verifyAcquirePermit(controller, CONCURRENT_NS, 3, 0);
    assertEquals(0, controller.getTotalElasticAvailableHandler());
  }

  private Configuration createConf(int handlers) {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFS_ROUTER_HANDLER_COUNT_KEY, handlers);
    conf.set(DFS_ROUTER_MONITOR_NAMENODE, NAMESERVICES);
    conf.setClass(
        RBFConfigKeys.DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS,
        ElasticRouterRpcFairnessPolicyController.class,
        RouterRpcFairnessPolicyController.class);
    return conf;
  }
}
