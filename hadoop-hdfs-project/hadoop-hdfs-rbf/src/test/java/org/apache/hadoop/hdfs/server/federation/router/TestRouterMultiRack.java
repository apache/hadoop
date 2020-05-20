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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.ECTopologyVerifierResult;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test class with clusters having multiple racks.
 */
public class TestRouterMultiRack {
  private static StateStoreDFSCluster cluster;
  private static RouterContext routerContext;
  private static DistributedFileSystem routerFs;
  private static NamenodeContext nnContext0;
  private static NamenodeContext nnContext1;
  private static DistributedFileSystem nnFs0;
  private static DistributedFileSystem nnFs1;

  @BeforeClass
  public static void setUp() throws Exception {

    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, 2,
        MultipleDestinationMountTableResolver.class);
    Configuration routerConf =
        new RouterConfigBuilder().stateStore().admin().quota().rpc().build();
    Configuration hdfsConf = new Configuration(false);
    cluster.addNamenodeOverrides(hdfsConf);
    cluster.addRouterOverrides(routerConf);
    cluster.setNumDatanodesPerNameservice(9);
    cluster.setIndependentDNs();
    cluster.setRacks(
        new String[] {"/rack1", "/rack1", "/rack1", "/rack2", "/rack2",
            "/rack2", "/rack3", "/rack3", "/rack3", "/rack4", "/rack4",
            "/rack4", "/rack5", "/rack5", "/rack5", "/rack6", "/rack6",
            "/rack6"});
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    routerContext = cluster.getRandomRouter();
    routerFs = (DistributedFileSystem) routerContext.getFileSystem();
    nnContext0 = cluster.getNamenode("ns0", null);
    nnContext1 = cluster.getNamenode("ns1", null);
    nnFs0 = (DistributedFileSystem) nnContext0.getFileSystem();
    nnFs1 = (DistributedFileSystem) nnContext1.getFileSystem();
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.stopRouter(routerContext);
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testGetECTopologyResultForPolicies() throws IOException {
    routerFs.enableErasureCodingPolicy("RS-6-3-1024k");
    // No policies specified should return result for the enabled policy.
    ECTopologyVerifierResult result = routerFs.getECTopologyResultForPolicies();
    assertTrue(result.isSupported());
    // Specified policy requiring more datanodes than present in
    // the actual cluster.
    result = routerFs.getECTopologyResultForPolicies("RS-10-4-1024k");
    assertFalse(result.isSupported());
    // Specify multiple policies with one policy requiring more datanodes than
    // present in the actual cluster
    result = routerFs
        .getECTopologyResultForPolicies("RS-10-4-1024k", "RS-3-2-1024k");
    assertFalse(result.isSupported());
    // Specify multiple policies that require datanodes equal or less then
    // present in the actual cluster
    result = routerFs
        .getECTopologyResultForPolicies("XOR-2-1-1024k", "RS-3-2-1024k");
    assertTrue(result.isSupported());
    // Specify multiple policies with one policy requiring more datanodes than
    // present in the actual cluster
    result = routerFs
        .getECTopologyResultForPolicies("RS-10-4-1024k", "RS-3-2-1024k");
    assertFalse(result.isSupported());
    // Enable a policy requiring more datanodes than present in
    // the actual cluster.
    routerFs.enableErasureCodingPolicy("RS-10-4-1024k");
    result = routerFs.getECTopologyResultForPolicies();
    assertFalse(result.isSupported());
    // Check without specifying any policy, with one cluster having
    // all supported, but one cluster having one unsupported policy. The
    nnFs0.disableErasureCodingPolicy("RS-10-4-1024k");
    nnFs1.enableErasureCodingPolicy("RS-10-4-1024k");
    result = routerFs.getECTopologyResultForPolicies();
    assertFalse(result.isSupported());
    nnFs1.disableErasureCodingPolicy("RS-10-4-1024k");
    nnFs0.enableErasureCodingPolicy("RS-10-4-1024k");
    result = routerFs.getECTopologyResultForPolicies();
    assertFalse(result.isSupported());
  }
}
