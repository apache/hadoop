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

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;

/**
 * The the RPC interface of the {@link getRouter()} implemented by
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
    for (String mount : subclusterResolver.getMountPoints(path)) {
      requiredPaths.add(mount);
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
}