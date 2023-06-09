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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.apache.hadoop.test.Whitebox.getInternalState;
import static org.apache.hadoop.test.Whitebox.setInternalState;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCMetrics;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;
import org.slf4j.event.Level;

/**
 * The RPC interface of the {@link getRouter()} implemented by
 * {@link RouterRpcServer}.
 */
public class TestRouterRpcMultiDestination extends TestRouterRpc {

  @Override
  public void testSetup() throws Exception {

    MiniRouterDFSCluster cluster = getCluster();

    // Create mock locations
    getCluster().installMockLocations();
    List<RouterContext> routers = cluster.getRouters();

    // Add extra location to the root mount / such that the root mount points:
    // /
    //   ns0 -> /
    //   ns1 -> /
    for (RouterContext rc : routers) {
      Router router = rc.getRouter();
      MockResolver resolver = (MockResolver) router.getSubclusterResolver();
      resolver.addLocation("/", cluster.getNameservices().get(1), "/");
    }

    // Create a mount that points to 2 dirs in the same ns:
    // /same
    //   ns0 -> /
    //   ns0 -> /target-ns0
    for (RouterContext rc : routers) {
      Router router = rc.getRouter();
      MockResolver resolver = (MockResolver) router.getSubclusterResolver();
      List<String> nss = cluster.getNameservices();
      String ns0 = nss.get(0);
      resolver.addLocation("/same", ns0, "/");
      resolver.addLocation("/same", ns0, cluster.getNamenodePathForNS(ns0));
    }

    // Delete all files via the NNs and verify
    cluster.deleteAllFiles();

    // Create test fixtures on NN
    cluster.createTestDirectoriesNamenode();

    // Wait to ensure NN has fully created its test directories
    Thread.sleep(100);

    // Pick a NS, namenode and getRouter() for this test
    RouterContext router = cluster.getRandomRouter();
    this.setRouter(router);

    String ns = cluster.getRandomNameservice();
    this.setNs(ns);
    this.setNamenode(cluster.getNamenode(ns, null));

    // Create a test file on a single NN that is accessed via a getRouter() path
    // with 2 destinations. All tests should failover to the alternate
    // destination if the wrong NN is attempted first.
    Random r = new Random();
    String randomString = "testfile-" + r.nextInt();
    setNamenodeFile("/" + randomString);
    setRouterFile("/" + randomString);

    FileSystem nnFs = getNamenodeFileSystem();
    FileSystem routerFs = getRouterFileSystem();
    createFile(nnFs, getNamenodeFile(), 32);

    verifyFileExists(nnFs, getNamenodeFile());
    verifyFileExists(routerFs, getRouterFile());
  }

  private void testListing(String path) throws IOException {

    // Collect the mount table entries for this path
    Set<String> requiredPaths = new TreeSet<>();
    RouterContext rc = getRouterContext();
    Router router = rc.getRouter();
    FileSubclusterResolver subclusterResolver = router.getSubclusterResolver();
    List<String> mountList = subclusterResolver.getMountPoints(path);
    if (mountList != null) {
      requiredPaths.addAll(mountList);
    }

    // Get files/dirs from the Namenodes
    PathLocation location = subclusterResolver.getDestinationForPath(path);
    for (RemoteLocation loc : location.getDestinations()) {
      String nsId = loc.getNameserviceId();
      String dest = loc.getDest();
      NamenodeContext nn = getCluster().getNamenode(nsId, null);
      FileSystem fs = nn.getFileSystem();
      FileStatus[] files = fs.listStatus(new Path(dest));
      for (FileStatus file : files) {
        String pathName = file.getPath().getName();
        requiredPaths.add(pathName);
      }
    }

    // Get files/dirs from the Router
    DirectoryListing listing =
        getRouterProtocol().getListing(path, HdfsFileStatus.EMPTY_NAME, false);
    Iterator<String> requiredPathsIterator = requiredPaths.iterator();

    // Match each path returned and verify order returned
    HdfsFileStatus[] partialListing = listing.getPartialListing();
    for (HdfsFileStatus fileStatus : listing.getPartialListing()) {
      String fileName = requiredPathsIterator.next();
      String currentFile = fileStatus.getFullPath(new Path(path)).getName();
      assertEquals(currentFile, fileName);
    }

    // Verify the total number of results found/matched
    assertEquals(
        requiredPaths + " doesn't match " + Arrays.toString(partialListing),
        requiredPaths.size(), partialListing.length);
  }

  /**
   * Verify the metric ProxyOp with RemoteException.
   */
  @Test
  public void testProxyOpWithRemoteException() throws IOException {
    final String testPath = "/proxy_op/remote_exception.txt";
    final FederationRPCMetrics metrics = getRouterContext().
        getRouter().getRpcServer().getRPCMetrics();
    String ns1 = getCluster().getNameservices().get(1);
    final FileSystem fileSystem1 = getCluster().
        getNamenode(ns1, null).getFileSystem();

    try {
      // Create the test file in ns1.
      createFile(fileSystem1, testPath, 32);

      long beforeProxyOp = metrics.getProxyOps();
      // First retry nn0 with remoteException then nn1.
      getRouterProtocol().getBlockLocations(testPath, 0, 1);
      assertEquals(2, metrics.getProxyOps() - beforeProxyOp);
    } finally {
      fileSystem1.delete(new Path(testPath), true);
    }
  }

  @Override
  public void testProxyListFiles() throws IOException, InterruptedException,
      URISyntaxException, NoSuchMethodException, SecurityException {

    // Verify that the root listing is a union of the mount table destinations
    // and the files stored at all nameservices mounted at the root (ns0 + ns1)
    // / -->
    // /ns0 (from mount table)
    // /ns1 (from mount table)
    // /same (from the mount table)
    // all items in / of ns0 from mapping of / -> ns0:::/)
    // all items in / of ns1 from mapping of / -> ns1:::/)
    testListing("/");

    // Verify that the "/same" mount point lists the contents of both dirs in
    // the same ns
    // /same -->
    // /target-ns0 (from root of ns0)
    // /testdir (from contents of /target-ns0)
    testListing("/same");

    // List a non-existing path and validate error response with NN behavior
    ClientProtocol namenodeProtocol =
        getCluster().getRandomNamenode().getClient().getNamenode();
    Method m = ClientProtocol.class.getMethod(
        "getListing", String.class,  byte[].class, boolean.class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(getRouterProtocol(), namenodeProtocol, m,
        new Object[] {badPath, HdfsFileStatus.EMPTY_NAME, false});
  }

  @Override
  public void testProxyRenameFiles() throws IOException, InterruptedException {

    super.testProxyRenameFiles();

    List<String> nss = getCluster().getNameservices();
    String ns0 = nss.get(0);
    String ns1 = nss.get(1);

    // Rename a file from ns0 into the root (mapped to both ns0 and ns1)
    String testDir0 = getCluster().getFederatedTestDirectoryForNS(ns0);
    String filename0 = testDir0 + "/testrename";
    String renamedFile = "/testrename";
    testRename(getRouterContext(), filename0, renamedFile, false);
    testRename2(getRouterContext(), filename0, renamedFile, false);

    // Rename a file from ns1 into the root (mapped to both ns0 and ns1)
    String testDir1 = getCluster().getFederatedTestDirectoryForNS(ns1);
    String filename1 = testDir1 + "/testrename";
    testRename(getRouterContext(), filename1, renamedFile, false);
    testRename2(getRouterContext(), filename1, renamedFile, false);
  }

  /**
   * Verify some rpc with previous block not null.
   */
  @Test
  public void testPreviousBlockNotNull()
      throws IOException, URISyntaxException {
    final GenericTestUtils.LogCapturer stateChangeLog =
        GenericTestUtils.LogCapturer.captureLogs(NameNode.stateChangeLog);
    GenericTestUtils.setLogLevel(NameNode.stateChangeLog, Level.DEBUG);

    final GenericTestUtils.LogCapturer nameNodeLog =
        GenericTestUtils.LogCapturer.captureLogs(NameNode.LOG);
    GenericTestUtils.setLogLevel(NameNode.LOG, Level.DEBUG);

    final FederationRPCMetrics metrics = getRouterContext().
        getRouter().getRpcServer().getRPCMetrics();
    final ClientProtocol clientProtocol = getRouterProtocol();
    final EnumSet<CreateFlag> createFlag = EnumSet.of(CreateFlag.CREATE,
        CreateFlag.OVERWRITE);
    final String clientName = getRouterContext().getClient().getClientName();
    final String testPath = "/getAdditionalData/test.txt";
    final String ns1 = getCluster().getNameservices().get(1);
    final FileSystem fileSystem1 = getCluster().
        getNamenode(ns1, null).getFileSystem();

    try {
      // Create the test file in NS1.
      createFile(fileSystem1, testPath, 32);

      // Crate the test file via Router to get file status.
      HdfsFileStatus status = clientProtocol.create(
          testPath, new FsPermission("777"), clientName,
          new EnumSetWritable<>(createFlag), true, (short) 1,
          (long) 1024, CryptoProtocolVersion.supported(), null, null);
      long proxyNumCreate = metrics.getProcessingOps();

      // Add a block via router and previous block is null.
      LocatedBlock blockOne = clientProtocol.addBlock(
          testPath, clientName, null, null,
          status.getFileId(), null, null);
      assertNotNull(blockOne);
      long proxyNumAddBlock = metrics.getProcessingOps();
      assertEquals(2, proxyNumAddBlock - proxyNumCreate);

      stateChangeLog.clearOutput();
      // Add a block via router and previous block is not null.
      LocatedBlock blockTwo = clientProtocol.addBlock(
          testPath, clientName, blockOne.getBlock(), null,
          status.getFileId(), null, null);
      assertNotNull(blockTwo);
      long proxyNumAddBlock2 = metrics.getProcessingOps();
      assertEquals(1, proxyNumAddBlock2 - proxyNumAddBlock);
      assertTrue(stateChangeLog.getOutput().contains("BLOCK* getAdditionalBlock: " + testPath));

      nameNodeLog.clearOutput();
      // Get additionalDatanode via router and block is not null.
      DatanodeInfo[] exclusions = DatanodeInfo.EMPTY_ARRAY;
      LocatedBlock newBlock = clientProtocol.getAdditionalDatanode(
          testPath, status.getFileId(), blockTwo.getBlock(),
          blockTwo.getLocations(), blockTwo.getStorageIDs(), exclusions,
          1, clientName);
      assertNotNull(newBlock);
      long proxyNumAdditionalDatanode = metrics.getProcessingOps();
      assertEquals(1, proxyNumAdditionalDatanode - proxyNumAddBlock2);
      assertTrue(nameNodeLog.getOutput().contains("getAdditionalDatanode: src=" + testPath));

      stateChangeLog.clearOutput();
      // Complete the file via router and last block is not null.
      clientProtocol.complete(testPath, clientName,
          newBlock.getBlock(), status.getFileId());
      long proxyNumComplete = metrics.getProcessingOps();
      assertEquals(1, proxyNumComplete - proxyNumAdditionalDatanode);
      assertTrue(stateChangeLog.getOutput().contains("DIR* NameSystem.completeFile: " + testPath));
    } finally {
      clientProtocol.delete(testPath, true);
    }
  }

  /**
   * Test recoverLease when the result is false.
   */
  @Test
  public void testRecoverLease() throws Exception {
    Path testPath = new Path("/recovery/test_recovery_lease");
    DistributedFileSystem routerFs =
        (DistributedFileSystem) getRouterFileSystem();
    FSDataOutputStream fsDataOutputStream = null;
    try {
      fsDataOutputStream = routerFs.create(testPath);
      fsDataOutputStream.write("hello world".getBytes());
      fsDataOutputStream.hflush();

      boolean result = routerFs.recoverLease(testPath);
      assertFalse(result);
    } finally {
      IOUtils.closeStream(fsDataOutputStream);
      routerFs.delete(testPath, true);
    }
  }

  /**
   * Test isFileClosed when the result is false.
   */
  @Test
  public void testIsFileClosed() throws Exception {
    Path testPath = new Path("/is_file_closed.txt");
    DistributedFileSystem routerFs =
        (DistributedFileSystem) getRouterFileSystem();
    FSDataOutputStream fsDataOutputStream = null;
    try {
      fsDataOutputStream = routerFs.create(testPath);
      fsDataOutputStream.write("hello world".getBytes());
      fsDataOutputStream.hflush();

      boolean result = routerFs.isFileClosed(testPath);
      assertFalse(result);
    } finally {
      IOUtils.closeStream(fsDataOutputStream);
      routerFs.delete(testPath, true);
    }
  }

  @Test
  public void testGetContentSummaryEc() throws Exception {
    DistributedFileSystem routerDFS =
        (DistributedFileSystem) getRouterFileSystem();
    Path dir = new Path("/");
    String expectedECPolicy = "RS-6-3-1024k";
    try {
      routerDFS.setErasureCodingPolicy(dir, expectedECPolicy);
      assertEquals(expectedECPolicy,
          routerDFS.getContentSummary(dir).getErasureCodingPolicy());
    } finally {
      routerDFS.unsetErasureCodingPolicy(dir);
    }
  }

  @Test
  public void testSubclusterDown() throws Exception {
    final int totalFiles = 6;

    List<RouterContext> routers = getCluster().getRouters();

    // Test the behavior when everything is fine
    FileSystem fs = getRouterFileSystem();
    FileStatus[] files = fs.listStatus(new Path("/"));
    assertEquals(totalFiles, files.length);

    // Simulate one of the subclusters is in standby
    NameNode nn0 = getCluster().getNamenode("ns0", null).getNamenode();
    FSNamesystem ns0 = nn0.getNamesystem();
    HAContext nn0haCtx = (HAContext)getInternalState(ns0, "haContext");
    HAContext mockCtx = mock(HAContext.class);
    doThrow(new StandbyException("Mock")).when(mockCtx).checkOperation(any());
    setInternalState(ns0, "haContext", mockCtx);

    // router0 should throw an exception
    RouterContext router0 = routers.get(0);
    RouterRpcServer router0RPCServer = router0.getRouter().getRpcServer();
    RouterClientProtocol router0ClientProtocol =
        router0RPCServer.getClientProtocolModule();
    setInternalState(router0ClientProtocol, "allowPartialList", false);
    try {
      router0.getFileSystem().listStatus(new Path("/"));
      fail("I should throw an exception");
    } catch (RemoteException re) {
      GenericTestUtils.assertExceptionContains(
          "No namenode available to invoke getListing", re);
    }

    // router1 should report partial results
    RouterContext router1 = routers.get(1);
    files = router1.getFileSystem().listStatus(new Path("/"));
    assertTrue("Found " + files.length + " items, we should have less",
        files.length < totalFiles);


    // Restore the HA context and the Router
    setInternalState(ns0, "haContext", nn0haCtx);
    setInternalState(router0ClientProtocol, "allowPartialList", true);
  }

  @Test
  public void testCallerContextWithMultiDestinations() throws IOException {
    GenericTestUtils.LogCapturer auditLog =
        GenericTestUtils.LogCapturer.captureLogs(FSNamesystem.AUDIT_LOG);

    // set client context
    CallerContext.setCurrent(
        new CallerContext.Builder("clientContext").build());
    // assert the initial caller context as expected
    assertEquals("clientContext", CallerContext.getCurrent().getContext());

    DistributedFileSystem routerFs =
        (DistributedFileSystem) getRouterFileSystem();
    // create a directory via the router
    Path dirPath = new Path("/test_caller_context_with_multi_destinations");
    routerFs.mkdirs(dirPath);
    // invoke concurrently in RouterRpcClient
    routerFs.listStatus(dirPath);
    // invoke sequentially in RouterRpcClient
    routerFs.getFileStatus(dirPath);

    String auditFlag = "src=" + dirPath.toString();
    String clientIpInfo = "clientIp:"
        + InetAddress.getLocalHost().getHostAddress();
    for (String line : auditLog.getOutput().split("\n")) {
      if (line.contains(auditFlag)) {
        // assert origin caller context exist in audit log
        String callerContext = line.substring(line.indexOf("callerContext="));
        assertTrue(callerContext.contains("clientContext"));
        // assert client ip info exist in caller context
        assertTrue(callerContext.contains(clientIpInfo));
        // assert client ip info appears only once in caller context
        assertEquals(callerContext.indexOf(clientIpInfo),
            callerContext.lastIndexOf(clientIpInfo));
      }
    }
    // clear client context
    CallerContext.setCurrent(null);
  }
}