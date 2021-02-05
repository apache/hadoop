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

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createFile;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.verifyFileExists;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FEDERATION_RENAME_BANDWIDTH;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FEDERATION_RENAME_MAP;
import static org.apache.hadoop.test.GenericTestUtils.getMethodName;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.SCHEDULER_JOURNAL_URI;
import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.tools.fedbalance.DistCpProcedure;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Basic tests of router federation rename. Rename across namespaces.
 */
public class TestRouterFederationRename {

  private static final int NUM_SUBCLUSTERS = 2;
  private static final int NUM_DNS = 6;

  /** Federated HDFS cluster. */
  private static MiniRouterDFSCluster cluster;

  /** Random Router for this federated cluster. */
  private RouterContext router;

  /** Random nameservice in the federated cluster.  */
  private String ns;
  /** Filesystem interface to the Router. */
  private FileSystem routerFS;
  /** Filesystem interface to the Namenode. */
  private FileSystem nnFS;
  /** File in the Namenode. */
  private String nnFile;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    Configuration namenodeConf = new Configuration();
    namenodeConf.setBoolean(DFSConfigKeys.HADOOP_CALLER_CONTEXT_ENABLED_KEY,
        true);
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

  @AfterClass
  public static void tearDown() {
    cluster.shutdown();
    DistCpProcedure.disableForTest();
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
    RouterContext rndRouter = cluster.getRandomRouter();
    this.setRouter(rndRouter);

    // Create a mount that points to 2 dirs in the same ns:
    // /same
    //   ns0 -> /
    //   ns0 -> /target-ns0
    for (RouterContext rc : cluster.getRouters()) {
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

  protected void createDir(FileSystem fs, String dir) throws IOException {
    fs.mkdirs(new Path(dir));
    String file = dir + "/file";
    createFile(fs, file, 32);
    verifyFileExists(fs, dir);
    verifyFileExists(fs, file);
  }

  protected void testRenameDir(RouterContext testRouter, String path,
      String renamedPath, boolean exceptionExpected, Callable<Object> call)
      throws IOException {
    createDir(testRouter.getFileSystem(), path);
    // rename
    boolean exceptionThrown = false;
    try {
      call.call();
      assertFalse(verifyFileExists(testRouter.getFileSystem(), path));
      assertTrue(
          verifyFileExists(testRouter.getFileSystem(), renamedPath + "/file"));
    } catch (Exception ex) {
      exceptionThrown = true;
      assertTrue(verifyFileExists(testRouter.getFileSystem(), path + "/file"));
      assertFalse(verifyFileExists(testRouter.getFileSystem(), renamedPath));
    } finally {
      FileContext fileContext = testRouter.getFileContext();
      fileContext.delete(new Path(path), true);
      fileContext.delete(new Path(renamedPath), true);
    }
    if (exceptionExpected) {
      // Error was expected.
      assertTrue(exceptionThrown);
    } else {
      // No error was expected.
      assertFalse(exceptionThrown);
    }
  }

  protected void setRouter(RouterContext r) throws IOException {
    this.router = r;
    this.routerFS = r.getFileSystem();
  }

  protected void setNs(String nameservice) {
    this.ns = nameservice;
  }

  protected void setNamenode(NamenodeContext nn) throws IOException {
    this.nnFS = nn.getFileSystem();
  }

  protected FileSystem getRouterFileSystem() {
    return this.routerFS;
  }

  @Test
  public void testSuccessfulRbfRename() throws Exception {
    List<String> nss = cluster.getNameservices();
    String ns0 = nss.get(0);
    String ns1 = nss.get(1);

    // Test successfully rename a dir to a destination that is in a different
    // namespace.
    String dir =
        cluster.getFederatedTestDirectoryForNS(ns0) + "/" + getMethodName();
    String renamedDir =
        cluster.getFederatedTestDirectoryForNS(ns1) + "/" + getMethodName();
    testRenameDir(router, dir, renamedDir, false, () -> {
      DFSClient client = router.getClient();
      ClientProtocol clientProtocol = client.getNamenode();
      clientProtocol.rename(dir, renamedDir);
      return null;
    });
    testRenameDir(router, dir, renamedDir, false, () -> {
      DFSClient client = router.getClient();
      ClientProtocol clientProtocol = client.getNamenode();
      clientProtocol.rename2(dir, renamedDir);
      return null;
    });
  }

  @Test
  public void testRbfRenameFile() throws Exception {
    List<String> nss = cluster.getNameservices();
    String ns0 = nss.get(0);
    String ns1 = nss.get(1);

    // Test router federation rename a file.
    String file =
        cluster.getFederatedTestDirectoryForNS(ns0) + "/" + getMethodName();
    String renamedFile =
        cluster.getFederatedTestDirectoryForNS(ns1) + "/" + getMethodName();
    createFile(routerFS, file, 32);
    getRouterFileSystem().mkdirs(new Path(renamedFile));
    LambdaTestUtils.intercept(RemoteException.class, "should be a directory",
        "Expect RemoteException.", () -> {
          DFSClient client = router.getClient();
          ClientProtocol clientProtocol = client.getNamenode();
          clientProtocol.rename(file, renamedFile);
          return null;
        });
    LambdaTestUtils.intercept(RemoteException.class, "should be a directory",
        "Expect RemoteException.", () -> {
          DFSClient client = router.getClient();
          ClientProtocol clientProtocol = client.getNamenode();
          clientProtocol.rename2(file, renamedFile);
          return null;
        });
    getRouterFileSystem().delete(new Path(file), true);
    getRouterFileSystem().delete(new Path(renamedFile), true);
  }

  @Test
  public void testRbfRenameWhenDstAlreadyExists() throws Exception {
    List<String> nss = cluster.getNameservices();
    String ns0 = nss.get(0);
    String ns1 = nss.get(1);

    // Test router federation rename a path to a destination that is in a
    // different namespace and already exists.
    String dir =
        cluster.getFederatedTestDirectoryForNS(ns0) + "/" + getMethodName();
    String renamedDir =
        cluster.getFederatedTestDirectoryForNS(ns1) + "/" + getMethodName();
    createDir(routerFS, dir);
    getRouterFileSystem().mkdirs(new Path(renamedDir));
    LambdaTestUtils.intercept(RemoteException.class, "already exists",
        "Expect RemoteException.", () -> {
          DFSClient client = router.getClient();
          ClientProtocol clientProtocol = client.getNamenode();
          clientProtocol.rename(dir, renamedDir);
          return null;
        });
    LambdaTestUtils.intercept(RemoteException.class, "already exists",
        "Expect RemoteException.", () -> {
          DFSClient client = router.getClient();
          ClientProtocol clientProtocol = client.getNamenode();
          clientProtocol.rename2(dir, renamedDir);
          return null;
        });
    getRouterFileSystem().delete(new Path(dir), true);
    getRouterFileSystem().delete(new Path(renamedDir), true);
  }

  @Test
  public void testRbfRenameWhenSrcNotExists() throws Exception {
    List<String> nss = cluster.getNameservices();
    String ns0 = nss.get(0);
    String ns1 = nss.get(1);

    // Test router federation rename un-existed path.
    String dir =
        cluster.getFederatedTestDirectoryForNS(ns0) + "/" + getMethodName();
    String renamedDir =
        cluster.getFederatedTestDirectoryForNS(ns1) + "/" + getMethodName();
    LambdaTestUtils.intercept(RemoteException.class, "File does not exist",
        "Expect RemoteException.", () -> {
          DFSClient client = router.getClient();
          ClientProtocol clientProtocol = client.getNamenode();
          clientProtocol.rename(dir, renamedDir);
          return null;
        });
    LambdaTestUtils.intercept(RemoteException.class, "File does not exist",
        "Expect RemoteException.", () -> {
          DFSClient client = router.getClient();
          ClientProtocol clientProtocol = client.getNamenode();
          clientProtocol.rename2(dir, renamedDir);
          return null;
        });
  }

  @Test
  public void testRbfRenameOfMountPoint() throws Exception {
    List<String> nss = cluster.getNameservices();
    String ns0 = nss.get(0);
    String ns1 = nss.get(1);

    // Test router federation rename a mount point.
    String dir = cluster.getFederatedPathForNS(ns0);
    String renamedDir = cluster.getFederatedPathForNS(ns1);
    LambdaTestUtils.intercept(RemoteException.class, "is a mount point",
        "Expect RemoteException.", () -> {
          DFSClient client = router.getClient();
          ClientProtocol clientProtocol = client.getNamenode();
          clientProtocol.rename(dir, renamedDir);
          return null;
        });
    LambdaTestUtils.intercept(RemoteException.class, "is a mount point",
        "Expect RemoteException.", () -> {
          DFSClient client = router.getClient();
          ClientProtocol clientProtocol = client.getNamenode();
          clientProtocol.rename2(dir, renamedDir);
          return null;
        });
  }

  @Test
  public void testRbfRenameWithMultiDestination() throws Exception {
    List<String> nss = cluster.getNameservices();
    String ns1 = nss.get(1);
    FileSystem rfs = getRouterFileSystem();

    // Test router federation rename a path with multi-destination.
    String dir = "/same/" + getMethodName();
    String renamedDir = cluster.getFederatedTestDirectoryForNS(ns1) + "/"
        + getMethodName();
    createDir(rfs, dir);
    getRouterFileSystem().mkdirs(new Path(renamedDir));
    LambdaTestUtils.intercept(RemoteException.class,
        "The remote location should be exactly one", "Expect RemoteException.",
        () -> {
          DFSClient client = router.getClient();
          ClientProtocol clientProtocol = client.getNamenode();
          clientProtocol.rename(dir, renamedDir);
          return null;
        });
    LambdaTestUtils.intercept(RemoteException.class,
        "The remote location should be exactly one", "Expect RemoteException.",
        () -> {
          DFSClient client = router.getClient();
          ClientProtocol clientProtocol = client.getNamenode();
          clientProtocol.rename2(dir, renamedDir);
          return null;
        });
    getRouterFileSystem().delete(new Path(dir), true);
    getRouterFileSystem().delete(new Path(renamedDir), true);
  }

  @Test(timeout = 10000)
  public void testCounter() throws Exception {
    final RouterRpcServer rpcServer = router.getRouter().getRpcServer();
    List<String> nss = cluster.getNameservices();
    String ns0 = nss.get(0);
    String ns1 = nss.get(1);
    RouterFederationRename rbfRename =
        Mockito.spy(new RouterFederationRename(rpcServer, router.getConf()));
    String path = "/src";
    createDir(cluster.getCluster().getFileSystem(0), path);
    // Watch the scheduler job count.
    int expectedSchedulerCount = rpcServer.getSchedulerJobCount() + 1;
    AtomicInteger maxSchedulerCount = new AtomicInteger();
    AtomicBoolean watch = new AtomicBoolean(true);
    Thread watcher = new Thread(() -> {
      while (watch.get()) {
        int schedulerCount = rpcServer.getSchedulerJobCount();
        if (schedulerCount > maxSchedulerCount.get()) {
          maxSchedulerCount.set(schedulerCount);
        }
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
        }
      }
    });
    watcher.start();
    // Trigger rename.
    rbfRename.routerFedRename("/src", "/dst",
        Arrays.asList(new RemoteLocation(ns0, path, null)),
        Arrays.asList(new RemoteLocation(ns1, path, null)));
    // Verify count.
    verify(rbfRename).countIncrement();
    verify(rbfRename).countDecrement();
    watch.set(false);
    watcher.interrupt();
    watcher.join();
    assertEquals(expectedSchedulerCount, maxSchedulerCount.get());
    // Clean up.
    assertFalse(cluster.getCluster().getFileSystem(0).exists(new Path(path)));
    assertTrue(
        cluster.getCluster().getFileSystem(1).delete(new Path(path), true));
  }
}