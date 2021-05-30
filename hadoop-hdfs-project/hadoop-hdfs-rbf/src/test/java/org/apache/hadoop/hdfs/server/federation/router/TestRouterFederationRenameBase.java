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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createFile;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.verifyFileExists;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FEDERATION_RENAME_BANDWIDTH;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FEDERATION_RENAME_MAP;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ADMIN_ENABLE;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.SCHEDULER_JOURNAL_URI;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.tools.fedbalance.DistCpProcedure;

/**
 * Test base of router federation rename.
 */
public class TestRouterFederationRenameBase {

  static final int NUM_SUBCLUSTERS = 2;
  static final int NUM_DNS = 6;

  /** Random Router for this federated cluster. */
  private MiniRouterDFSCluster.RouterContext router;

  /** Random nameservice in the federated cluster.  */
  private String ns;
  /** Filesystem interface to the Router. */
  private FileSystem routerFS;
  /** Filesystem interface to the Namenode. */
  private FileSystem nnFS;
  /** File in the Namenode. */
  private String nnFile;

  /** Federated HDFS cluster. */
  private static MiniRouterDFSCluster cluster;

  public static void globalSetUp() throws Exception {
    Configuration namenodeConf = new Configuration();
    namenodeConf.setBoolean(DFSConfigKeys.HADOOP_CALLER_CONTEXT_ENABLED_KEY,
        true);
    namenodeConf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        TestRouterFederationRename.MockGroupsMapping.class.getName());
    cluster = new MiniRouterDFSCluster(false, NUM_SUBCLUSTERS);
    cluster.setNumDatanodesPerNameservice(NUM_DNS);
    cluster.addNamenodeOverrides(namenodeConf);
    cluster.setIndependentDNs();

    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_LIST_LIMIT, 5);
    cluster.addNamenodeOverrides(conf);
    // Start NNs and DNs and wait until ready.
    cluster.startCluster();

    // Start routers, enable router federation rename.
    String journal = "hdfs://" + cluster.getCluster().getNameNode(1)
        .getClientNamenodeAddress() + "/journal";
    Configuration routerConf = new RouterConfigBuilder()
        .metrics()
        .rpc()
        .routerRenameOption()
        .set(SCHEDULER_JOURNAL_URI, journal)
        .set(DFS_ROUTER_FEDERATION_RENAME_MAP, "1")
        .set(DFS_ROUTER_FEDERATION_RENAME_BANDWIDTH, "1")
        .build();
    // We decrease the DN cache times to make the test faster.
    routerConf.setTimeDuration(
        RBFConfigKeys.DN_REPORT_CACHE_EXPIRE, 1, TimeUnit.SECONDS);
    routerConf.setBoolean(DFS_ROUTER_ADMIN_ENABLE, true);
    routerConf.setBoolean(DFS_PERMISSIONS_ENABLED_KEY, true);
    routerConf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        TestRouterFederationRename.MockGroupsMapping.class.getName());
    cluster.addRouterOverrides(routerConf);
    cluster.startRouters();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();

    // We decrease the DN heartbeat expire interval to make them dead faster
    cluster.getCluster().getNamesystem(0).getBlockManager()
        .getDatanodeManager().setHeartbeatInterval(1);
    cluster.getCluster().getNamesystem(1).getBlockManager()
        .getDatanodeManager().setHeartbeatInterval(1);
    cluster.getCluster().getNamesystem(0).getBlockManager()
        .getDatanodeManager().setHeartbeatExpireInterval(3000);
    cluster.getCluster().getNamesystem(1).getBlockManager()
        .getDatanodeManager().setHeartbeatExpireInterval(3000);
    DistCpProcedure.enableForTest();
  }

  public static void tearDown() {
    cluster.shutdown();
    cluster = null;
    DistCpProcedure.disableForTest();
  }

  protected void setup() throws IOException, InterruptedException {

    // Create mock locations
    cluster.installMockLocations();

    // Delete all files via the NNs and verify
    cluster.deleteAllFiles();

    // Create test fixtures on NN
    cluster.createTestDirectoriesNamenode();

    // Random router for this test
    MiniRouterDFSCluster.RouterContext rndRouter = cluster.getRandomRouter();
    this.setRouter(rndRouter);

    // Create a mount that points to 2 dirs in the same ns:
    // /same
    //   ns0 -> /
    //   ns0 -> /target-ns0
    for (MiniRouterDFSCluster.RouterContext rc : cluster.getRouters()) {
      Router r = rc.getRouter();
      MockResolver resolver = (MockResolver) r.getSubclusterResolver();
      List<String> nss = cluster.getNameservices();
      String ns0 = nss.get(0);
      resolver.addLocation("/same", ns0, "/");
      resolver.addLocation("/same", ns0, cluster.getNamenodePathForNS(ns0));
    }

    // Pick a namenode for this test
    String ns0 = cluster.getNameservices().get(0);
    this.setNs(ns0);
    this.setNamenode(cluster.getNamenode(ns0, null));

    // Create a test file on the NN
    Random rnd = new Random();
    String randomFile = "testfile-" + rnd.nextInt();
    this.nnFile =
        cluster.getNamenodeTestDirectoryForNS(ns) + "/" + randomFile;

    createFile(nnFS, nnFile, 32);
    verifyFileExists(nnFS, nnFile);
  }

  protected void setRouter(MiniRouterDFSCluster.RouterContext r) throws
      IOException {
    this.router = r;
    this.routerFS = r.getFileSystem();
  }

  protected void setNs(String nameservice) {
    this.ns = nameservice;
  }

  protected void setNamenode(MiniRouterDFSCluster.NamenodeContext nn)
      throws IOException {
    this.nnFS = nn.getFileSystem();
  }

  protected FileSystem getRouterFileSystem() {
    return this.routerFS;
  }

  protected void createDir(FileSystem fs, String dir) throws IOException {
    fs.mkdirs(new Path(dir));
    String file = dir + "/file";
    createFile(fs, file, 32);
    verifyFileExists(fs, dir);
    verifyFileExists(fs, file);
  }

  public MiniRouterDFSCluster getCluster() {
    return cluster;
  }

  public MiniRouterDFSCluster.RouterContext getRouterContext() {
    return router;
  }
}
