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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Test the service that msync to all nameservices.
 */
public class TestRouterAutoMsyncService {

  private static MiniRouterDFSCluster cluster;
  private static Router router;
  private static RouterAutoMsyncService service;
  private static long msyncInterval = 1000;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void globalSetUp() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_AUTO_MSYNC_ENABLE, true);
    conf.setLong(RBFConfigKeys.DFS_ROUTER_AUTO_MSYNC_INTERVAL_MS, msyncInterval);
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_OBSERVER_READ_DEFAULT_KEY, true);

    cluster = new MiniRouterDFSCluster(true, 1, conf);

    // Start NNs and DNs and wait until ready
    cluster.startCluster(conf);
    cluster.startRouters();
    cluster.waitClusterUp();

    // Making one Namenodes active per nameservice
    if (cluster.isHighAvailability()) {
      for (String ns : cluster.getNameservices()) {
        cluster.switchToActive(ns, NAMENODES[0]);
        cluster.switchToStandby(ns, NAMENODES[1]);
      }
    }
    cluster.waitActiveNamespaces();

    router = cluster.getRandomRouter().getRouter();
    service = router.getRouterAutoMsyncService();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    cluster.shutdown();
    service.stop();
    service.close();
  }

  @Test
  public void testMsync() throws InterruptedException, TimeoutException {
    GenericTestUtils.waitFor(() -> {
      long ops = router.getRouterClientMetrics().getMsyncOps();
      return ops >= 1;
    }, 500, msyncInterval);
  }
}
