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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.metrics.NamenodeBeanMetrics;
import org.apache.hadoop.hdfs.server.namenode.sps.Context;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier;
import org.apache.hadoop.hdfs.server.sps.ExternalSPSContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Test StoragePolicySatisfy through router rpc calls.
 */
public class TestRouterRpcStoragePolicySatisfier {

  /** Federated HDFS cluster. */
  private static MiniRouterDFSCluster cluster;

  /** Client interface to the Router. */
  private static ClientProtocol routerProtocol;

  /** Filesystem interface to the Router. */
  private static FileSystem routerFS;
  /** Filesystem interface to the Namenode. */
  private static FileSystem nnFS;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    cluster = new MiniRouterDFSCluster(false, 1);
    // Set storage types for the cluster
    StorageType[][] newtypes = new StorageType[][] {
        {StorageType.ARCHIVE, StorageType.DISK}};
    cluster.setStorageTypes(newtypes);

    Configuration conf = cluster.getNamenodes().get(0).getConf();
    conf.set(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        HdfsConstants.StoragePolicySatisfierMode.EXTERNAL.toString());
    // Reduced refresh cycle to update latest datanodes.
    conf.setLong(DFSConfigKeys.DFS_SPS_DATANODE_CACHE_REFRESH_INTERVAL_MS,
        1000);
    cluster.addNamenodeOverrides(conf);

    cluster.setNumDatanodesPerNameservice(1);

    // Start NNs and DNs and wait until ready
    cluster.startCluster();

    // Start routers with only an RPC service
    Configuration routerConf = new RouterConfigBuilder()
        .metrics()
        .rpc()
        .build();
    // We decrease the DN cache times to make the test faster
    routerConf.setTimeDuration(
        RBFConfigKeys.DN_REPORT_CACHE_EXPIRE, 1, TimeUnit.SECONDS);
    cluster.addRouterOverrides(routerConf);
    cluster.startRouters();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();

    // Create mock locations
    cluster.installMockLocations();

    // Random router for this test
    MiniRouterDFSCluster.RouterContext rndRouter = cluster.getRandomRouter();

    routerProtocol = rndRouter.getClient().getNamenode();
    routerFS = rndRouter.getFileSystem();
    nnFS = cluster.getNamenodes().get(0).getFileSystem();

    NameNodeConnector nnc = DFSTestUtil.getNameNodeConnector(conf,
        HdfsServerConstants.MOVER_ID_PATH, 1, false);

    StoragePolicySatisfier externalSps = new StoragePolicySatisfier(conf);
    Context externalCtxt = new ExternalSPSContext(externalSps, nnc);

    externalSps.init(externalCtxt);
    externalSps.start(HdfsConstants.StoragePolicySatisfierMode.EXTERNAL);
  }

  @AfterClass
  public static void tearDown() {
    cluster.shutdown();
  }

  @Test
  public void testStoragePolicySatisfier() throws Exception {
    final String file = "/testStoragePolicySatisfierCommand";
    short repl = 1;
    int size = 32;
    DFSTestUtil.createFile(routerFS, new Path(file), size, repl, 0);
    // Varify storage type is DISK
    DFSTestUtil.waitExpectedStorageType(file, StorageType.DISK, 1, 20000,
        (DistributedFileSystem) routerFS);
    // Set storage policy as COLD
    routerProtocol
        .setStoragePolicy(file, HdfsConstants.COLD_STORAGE_POLICY_NAME);
    // Verify storage policy is set properly
    BlockStoragePolicy storagePolicy = routerProtocol.getStoragePolicy(file);
    assertEquals(HdfsConstants.COLD_STORAGE_POLICY_NAME,
        storagePolicy.getName());
    // Invoke satisfy storage policy
    routerProtocol.satisfyStoragePolicy(file);
    // Verify storage type is ARCHIVE
    DFSTestUtil.waitExpectedStorageType(file, StorageType.ARCHIVE, 1, 20000,
        (DistributedFileSystem) routerFS);

    // Verify storage type via NN
    DFSTestUtil.waitExpectedStorageType(file, StorageType.ARCHIVE, 1, 20000,
        (DistributedFileSystem) nnFS);
  }
}
