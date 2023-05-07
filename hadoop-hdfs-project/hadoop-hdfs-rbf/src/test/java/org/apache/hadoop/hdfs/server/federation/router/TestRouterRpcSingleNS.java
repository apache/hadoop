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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createFile;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.verifyFileExists;

/**
 * The the RPC interface of the {@link Router} implemented by
 * {@link RouterRpcServer}.
 * Tests covering the functionality of RouterRPCServer with
 * single nameService.
 */
public class TestRouterRpcSingleNS {

  /**
   * Federated HDFS cluster.
   */
  private static MiniRouterDFSCluster cluster;

  /**
   * Random Router for this federated cluster.
   */
  private MiniRouterDFSCluster.RouterContext router;

  /**
   * Random nameservice in the federated cluster.
   */
  private String ns;
  /**
   * First namenode in the nameservice.
   */
  private MiniRouterDFSCluster.NamenodeContext namenode;

  /**
   * Client interface to the Router.
   */
  private ClientProtocol routerProtocol;
  /**
   * Client interface to the Namenode.
   */
  private ClientProtocol nnProtocol;

  /**
   * NameNodeProtocol interface to the Router.
   */
  private NamenodeProtocol routerNamenodeProtocol;
  /**
   * NameNodeProtocol interface to the Namenode.
   */
  private NamenodeProtocol nnNamenodeProtocol;

  /**
   * Filesystem interface to the Router.
   */
  private FileSystem routerFS;
  /**
   * Filesystem interface to the Namenode.
   */
  private FileSystem nnFS;

  /**
   * File in the Router.
   */
  private String routerFile;
  /**
   * File in the Namenode.
   */
  private String nnFile;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    cluster = new MiniRouterDFSCluster(false, 1);
    cluster.setNumDatanodesPerNameservice(2);

    // Start NNs and DNs and wait until ready
    cluster.startCluster();

    // Start routers with only an RPC service
    Configuration routerConf = new RouterConfigBuilder().metrics().rpc()
        .build();
    // We decrease the DN cache times to make the test faster
    routerConf.setTimeDuration(RBFConfigKeys.DN_REPORT_CACHE_EXPIRE, 1,
        TimeUnit.SECONDS);
    cluster.addRouterOverrides(routerConf);
    cluster.startRouters();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();
  }

  @AfterClass
  public static void tearDown() {
    cluster.shutdown();
  }

  @Before
  public void testSetup() throws Exception {

    // Create mock locations
    cluster.installMockLocations();

    // Delete all files via the NNs and verify
    cluster.deleteAllFiles();

    // Create test fixtures on NN
    cluster.createTestDirectoriesNamenode();

    // Wait to ensure NN has fully created its test directories
    Thread.sleep(100);

    // Random router for this test
    MiniRouterDFSCluster.RouterContext rndRouter = cluster.getRandomRouter();
    this.setRouter(rndRouter);

    // Pick a namenode for this test
    String ns0 = cluster.getNameservices().get(0);
    this.setNs(ns0);
    this.setNamenode(cluster.getNamenode(ns0, null));

    // Create a test file on the NN
    Random rnd = new Random();
    String randomFile = "testfile-" + rnd.nextInt();
    this.nnFile = cluster.getNamenodeTestDirectoryForNS(ns) + "/" + randomFile;
    this.routerFile = cluster.getFederatedTestDirectoryForNS(ns) + "/"
        + randomFile;

    createFile(nnFS, nnFile, 32);
    verifyFileExists(nnFS, nnFile);
  }

  protected void setRouter(MiniRouterDFSCluster.RouterContext r)
      throws IOException, URISyntaxException {
    this.router = r;
    this.routerProtocol = r.getClient().getNamenode();
    this.routerFS = r.getFileSystem();
    this.routerNamenodeProtocol = NameNodeProxies.createProxy(router.getConf(),
        router.getFileSystem().getUri(), NamenodeProtocol.class).getProxy();
  }

  protected void setNs(String nameservice) {
    this.ns = nameservice;
  }

  protected void setNamenode(MiniRouterDFSCluster.NamenodeContext nn)
      throws IOException, URISyntaxException {
    this.namenode = nn;
    this.nnProtocol = nn.getClient().getNamenode();
    this.nnFS = nn.getFileSystem();

    // Namenode from the default namespace
    String ns0 = cluster.getNameservices().get(0);
    MiniRouterDFSCluster.NamenodeContext nn0 = cluster.getNamenode(ns0, null);
    this.nnNamenodeProtocol = NameNodeProxies.createProxy(nn0.getConf(),
        nn0.getFileSystem().getUri(), NamenodeProtocol.class).getProxy();
  }

  @Test
  public void testGetCurrentTXIDandRollEdits() throws IOException {
    Long rollEdits = routerProtocol.rollEdits();
    Long currentTXID = routerProtocol.getCurrentEditLogTxid();

    assertEquals(rollEdits, currentTXID);
  }

  @Test
  public void testSaveNamespace() throws IOException {
    cluster.getCluster().getFileSystem()
        .setSafeMode(SafeModeAction.ENTER);
    Boolean saveNamespace = routerProtocol.saveNamespace(0, 0);

    assertTrue(saveNamespace);
  }
}