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

import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the SafeMode.
 */
public class TestSafeMode {

  /** Federated HDFS cluster. */
  private MiniRouterDFSCluster cluster;

  @Before
  public  void setup() throws Exception {
    cluster = new MiniRouterDFSCluster(true, 2);

    // Start NNs and DNs and wait until ready
    cluster.startCluster();

    // Start routers with only an RPC service
    cluster.startRouters();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();

    // Setup the mount table
    cluster.installMockLocations();

    // Making one Namenodes active per nameservice
    if (cluster.isHighAvailability()) {
      for (String ns : cluster.getNameservices()) {
        cluster.switchToActive(ns, NAMENODES[0]);
        cluster.switchToStandby(ns, NAMENODES[1]);
      }
    }
    cluster.waitActiveNamespaces();
  }

  @After
  public void teardown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testProxySetSafemode() throws Exception {
    RouterContext routerContext = cluster.getRandomRouter();
    ClientProtocol routerProtocol = routerContext.getClient().getNamenode();
    routerProtocol.setSafeMode(SafeModeAction.SAFEMODE_GET, true);
    routerProtocol.setSafeMode(SafeModeAction.SAFEMODE_GET, false);
  }
}
