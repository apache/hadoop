/*
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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.tools.fedbalance.FedBalanceConfigs;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createNamenodeReport;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.FEDERATION_STORE_MEMBERSHIP_EXPIRATION_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDFSRouter {

  @Test
  public void testDefaultConfigs() {
    Configuration configuration = DFSRouter.getConfiguration();
    String journalUri =
        configuration.get(FedBalanceConfigs.SCHEDULER_JOURNAL_URI);
    int workerThreads =
        configuration.getInt(FedBalanceConfigs.WORK_THREAD_NUM, -1);
    Assertions.assertEquals("hdfs://localhost:8020/tmp/procedure", journalUri);
    Assertions.assertEquals(10, workerThreads);
  }

  @Test
  public void testClearStaleNamespacesInRouterStateIdContext() throws Exception {
    Router testRouter = new Router();
    Configuration routerConfig = DFSRouter.getConfiguration();
    routerConfig.set(FEDERATION_STORE_MEMBERSHIP_EXPIRATION_MS, "2000");
    routerConfig.set(RBFConfigKeys.DFS_ROUTER_SAFEMODE_ENABLE, "false");
    // Mock resolver classes
    routerConfig.setClass(RBFConfigKeys.FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS,
        MockResolver.class, ActiveNamenodeResolver.class);
    routerConfig.setClass(RBFConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS,
        MockResolver.class, FileSubclusterResolver.class);

    testRouter.init(routerConfig);
    String nsID1 = "ns0";
    String nsID2 = "ns1";
    MockResolver resolver = (MockResolver)testRouter.getNamenodeResolver();
    resolver.registerNamenode(createNamenodeReport(nsID1, "nn1",
        HAServiceProtocol.HAServiceState.ACTIVE));
    resolver.registerNamenode(createNamenodeReport(nsID2, "nn1",
        HAServiceProtocol.HAServiceState.ACTIVE));

    RouterRpcServer rpcServer = testRouter.getRpcServer();

    rpcServer.getRouterStateIdContext().getNamespaceStateId(nsID1);
    rpcServer.getRouterStateIdContext().getNamespaceStateId(nsID2);

    resolver.disableNamespace(nsID1);
    Thread.sleep(3000);
    RouterStateIdContext context = rpcServer.getRouterStateIdContext();
    assertEquals(2, context.getNamespaceIdMap().size());

    testRouter.start();
    Thread.sleep(3000);
    // wait clear stale namespaces
    RouterStateIdContext routerStateIdContext = rpcServer.getRouterStateIdContext();
    int size = routerStateIdContext.getNamespaceIdMap().size();
    assertEquals(1, size);
    rpcServer.stop();
    rpcServer.close();
    testRouter.close();
  }
}