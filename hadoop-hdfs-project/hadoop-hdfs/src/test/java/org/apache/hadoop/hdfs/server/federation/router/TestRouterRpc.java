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

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.addDirectory;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.countContents;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createFile;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.deleteFile;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.getFileStatus;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.verifyFileExists;
import static org.apache.hadoop.hdfs.server.federation.RouterDFSCluster.TEST_STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.RouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.RouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service.STATE;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The the RPC interface of the {@link Router} implemented by
 * {@link RouterRpcServer}.
 */
public class TestRouterRpc {

  /** Federated HDFS cluster. */
  private static RouterDFSCluster cluster;

  /** Random Router for this federated cluster. */
  private RouterContext router;

  /** Random nameservice in the federated cluster.  */
  private String ns;
  /** First namenode in the nameservice. */
  private NamenodeContext namenode;

  /** Client interface to the Router. */
  private ClientProtocol routerProtocol;
  /** Client interface to the Namenode. */
  private ClientProtocol nnProtocol;

  /** Filesystem interface to the Router. */
  private FileSystem routerFS;
  /** Filesystem interface to the Namenode. */
  private FileSystem nnFS;

  /** File in the Router. */
  private String routerFile;
  /** File in the Namenode. */
  private String nnFile;


  @BeforeClass
  public static void globalSetUp() throws Exception {
    cluster = new RouterDFSCluster(false, 2);

    // Start NNs and DNs and wait until ready
    cluster.startCluster();

    // Start routers with only an RPC service
    cluster.addRouterOverrides((new RouterConfigBuilder()).rpc().build());
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

    // Pick a NS, namenode and router for this test
    this.router = cluster.getRandomRouter();
    this.ns = cluster.getRandomNameservice();
    this.namenode = cluster.getNamenode(ns, null);

    // Handles to the ClientProtocol interface
    this.routerProtocol = router.getClient().getNamenode();
    this.nnProtocol = namenode.getClient().getNamenode();

    // Handles to the filesystem client
    this.nnFS = namenode.getFileSystem();
    this.routerFS = router.getFileSystem();

    // Create a test file on the NN
    Random r = new Random();
    String randomFile = "testfile-" + r.nextInt();
    this.nnFile =
        cluster.getNamenodeTestDirectoryForNS(ns) + "/" + randomFile;
    this.routerFile =
        cluster.getFederatedTestDirectoryForNS(ns) + "/" + randomFile;

    createFile(nnFS, nnFile, 32);
    verifyFileExists(nnFS, nnFile);
  }

  @Test
  public void testRpcService() throws IOException {
    Router testRouter = new Router();
    List<String> nss = cluster.getNameservices();
    String ns0 = nss.get(0);
    Configuration routerConfig = cluster.generateRouterConfiguration(ns0, null);
    RouterRpcServer server = new RouterRpcServer(routerConfig, testRouter,
        testRouter.getNamenodeResolver(), testRouter.getSubclusterResolver());
    server.init(routerConfig);
    assertEquals(STATE.INITED, server.getServiceState());
    server.start();
    assertEquals(STATE.STARTED, server.getServiceState());
    server.stop();
    assertEquals(STATE.STOPPED, server.getServiceState());
    server.close();
    testRouter.close();
  }

  protected RouterDFSCluster getCluster() {
    return TestRouterRpc.cluster;
  }

  protected RouterContext getRouterContext() {
    return this.router;
  }

  protected void setRouter(RouterContext r)
      throws IOException, URISyntaxException {
    this.router = r;
    this.routerProtocol = r.getClient().getNamenode();
    this.routerFS = r.getFileSystem();
  }

  protected FileSystem getRouterFileSystem() {
    return this.routerFS;
  }

  protected FileSystem getNamenodeFileSystem() {
    return this.nnFS;
  }

  protected ClientProtocol getRouterProtocol() {
    return this.routerProtocol;
  }

  protected ClientProtocol getNamenodeProtocol() {
    return this.nnProtocol;
  }

  protected NamenodeContext getNamenode() {
    return this.namenode;
  }

  protected void setNamenodeFile(String filename) {
    this.nnFile = filename;
  }

  protected String getNamenodeFile() {
    return this.nnFile;
  }

  protected void setRouterFile(String filename) {
    this.routerFile = filename;
  }

  protected String getRouterFile() {
    return this.routerFile;
  }

  protected void setNamenode(NamenodeContext nn)
      throws IOException, URISyntaxException {
    this.namenode = nn;
    this.nnProtocol = nn.getClient().getNamenode();
    this.nnFS = nn.getFileSystem();
  }

  protected String getNs() {
    return this.ns;
  }

  protected void setNs(String nameservice) {
    this.ns = nameservice;
  }

  protected static void compareResponses(
      ClientProtocol protocol1, ClientProtocol protocol2,
      Method m, Object[] paramList) {

    Object return1 = null;
    Exception exception1 = null;
    try {
      return1 = m.invoke(protocol1, paramList);
    } catch (Exception ex) {
      exception1 = ex;
    }

    Object return2 = null;
    Exception exception2 = null;
    try {
      return2 = m.invoke(protocol2, paramList);
    } catch (Exception ex) {
      exception2 = ex;
    }

    assertEquals(return1, return2);
    if (exception1 == null && exception2 == null) {
      return;
    }

    assertEquals(
        exception1.getCause().getClass(),
        exception2.getCause().getClass());
  }

  @Test
  public void testProxyListFiles() throws IOException, InterruptedException,
      URISyntaxException, NoSuchMethodException, SecurityException {

    // Verify that the root listing is a union of the mount table destinations
    // and the files stored at all nameservices mounted at the root (ns0 + ns1)
    //
    // / -->
    // /ns0 (from mount table)
    // /ns1 (from mount table)
    // all items in / of ns0 (default NS)

    // Collect the mount table entries from the root mount point
    Set<String> requiredPaths = new TreeSet<>();
    FileSubclusterResolver fileResolver =
        router.getRouter().getSubclusterResolver();
    for (String mount : fileResolver.getMountPoints("/")) {
      requiredPaths.add(mount);
    }

    // Collect all files/dirs on the root path of the default NS
    String defaultNs = cluster.getNameservices().get(0);
    NamenodeContext nn = cluster.getNamenode(defaultNs, null);
    FileStatus[] iterator = nn.getFileSystem().listStatus(new Path("/"));
    for (FileStatus file : iterator) {
      requiredPaths.add(file.getPath().getName());
    }

    // Fetch listing
    DirectoryListing listing =
        routerProtocol.getListing("/", HdfsFileStatus.EMPTY_NAME, false);
    Iterator<String> requiredPathsIterator = requiredPaths.iterator();
    // Match each path returned and verify order returned
    for(HdfsFileStatus f : listing.getPartialListing()) {
      String fileName = requiredPathsIterator.next();
      String currentFile = f.getFullPath(new Path("/")).getName();
      assertEquals(currentFile, fileName);
    }

    // Verify the total number of results found/matched
    assertEquals(requiredPaths.size(), listing.getPartialListing().length);

    // List a path that doesn't exist and validate error response with NN
    // behavior.
    Method m = ClientProtocol.class.getMethod(
        "getListing", String.class, byte[].class, boolean.class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(routerProtocol, nnProtocol, m,
        new Object[] {badPath, HdfsFileStatus.EMPTY_NAME, false});
  }

  @Test
  public void testProxyListFilesWithConflict()
      throws IOException, InterruptedException {

    // Add a directory to the namespace that conflicts with a mount point
    NamenodeContext nn = cluster.getNamenode(ns, null);
    FileSystem nnFs = nn.getFileSystem();
    addDirectory(nnFs, cluster.getFederatedTestDirectoryForNS(ns));

    FileSystem routerFs = router.getFileSystem();
    int initialCount = countContents(routerFs, "/");

    // Root file system now for NS X:
    // / ->
    // /ns0 (mount table)
    // /ns1 (mount table)
    // /target-ns0 (the target folder for the NS0 mapped to /
    // /nsX (local directory that duplicates mount table)
    int newCount = countContents(routerFs, "/");
    assertEquals(initialCount, newCount);

    // Verify that each root path is readable and contains one test directory
    assertEquals(1, countContents(routerFs, cluster.getFederatedPathForNS(ns)));

    // Verify that real folder for the ns contains a single test directory
    assertEquals(1, countContents(nnFs, cluster.getNamenodePathForNS(ns)));

  }

  protected void testRename(RouterContext testRouter, String filename,
      String renamedFile, boolean exceptionExpected) throws IOException {

    createFile(testRouter.getFileSystem(), filename, 32);
    // verify
    verifyFileExists(testRouter.getFileSystem(), filename);
    // rename
    boolean exceptionThrown = false;
    try {
      DFSClient client = testRouter.getClient();
      ClientProtocol clientProtocol = client.getNamenode();
      clientProtocol.rename(filename, renamedFile);
    } catch (Exception ex) {
      exceptionThrown = true;
    }
    if (exceptionExpected) {
      // Error was expected
      assertTrue(exceptionThrown);
      FileContext fileContext = testRouter.getFileContext();
      assertTrue(fileContext.delete(new Path(filename), true));
    } else {
      // No error was expected
      assertFalse(exceptionThrown);
      // verify
      assertTrue(verifyFileExists(testRouter.getFileSystem(), renamedFile));
      // delete
      FileContext fileContext = testRouter.getFileContext();
      assertTrue(fileContext.delete(new Path(renamedFile), true));
    }
  }

  protected void testRename2(RouterContext testRouter, String filename,
      String renamedFile, boolean exceptionExpected) throws IOException {
    createFile(testRouter.getFileSystem(), filename, 32);
    // verify
    verifyFileExists(testRouter.getFileSystem(), filename);
    // rename
    boolean exceptionThrown = false;
    try {
      DFSClient client = testRouter.getClient();
      ClientProtocol clientProtocol = client.getNamenode();
      clientProtocol.rename2(filename, renamedFile, new Options.Rename[] {});
    } catch (Exception ex) {
      exceptionThrown = true;
    }
    assertEquals(exceptionExpected, exceptionThrown);
    if (exceptionExpected) {
      // Error was expected
      FileContext fileContext = testRouter.getFileContext();
      assertTrue(fileContext.delete(new Path(filename), true));
    } else {
      // verify
      assertTrue(verifyFileExists(testRouter.getFileSystem(), renamedFile));
      // delete
      FileContext fileContext = testRouter.getFileContext();
      assertTrue(fileContext.delete(new Path(renamedFile), true));
    }
  }

  @Test
  public void testProxyRenameFiles() throws IOException, InterruptedException {

    Thread.sleep(5000);
    List<String> nss = cluster.getNameservices();
    String ns0 = nss.get(0);
    String ns1 = nss.get(1);

    // Rename within the same namespace
    // /ns0/testdir/testrename -> /ns0/testdir/testrename-append
    String filename =
        cluster.getFederatedTestDirectoryForNS(ns0) + "/testrename";
    String renamedFile = filename + "-append";
    testRename(router, filename, renamedFile, false);
    testRename2(router, filename, renamedFile, false);

    // Rename a file to a destination that is in a different namespace (fails)
    filename = cluster.getFederatedTestDirectoryForNS(ns0) + "/testrename";
    renamedFile = cluster.getFederatedTestDirectoryForNS(ns1) + "/testrename";
    testRename(router, filename, renamedFile, true);
    testRename2(router, filename, renamedFile, true);
  }

  @Test
  public void testProxyChownFiles() throws Exception {

    String newUsername = "TestUser";
    String newGroup = "TestGroup";

    // change owner
    routerProtocol.setOwner(routerFile, newUsername, newGroup);

    // Verify with NN
    FileStatus file = getFileStatus(namenode.getFileSystem(), nnFile);
    assertEquals(file.getOwner(), newUsername);
    assertEquals(file.getGroup(), newGroup);

    // Bad request and validate router response matches NN response.
    Method m = ClientProtocol.class.getMethod("setOwner", String.class,
        String.class, String.class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(routerProtocol, nnProtocol, m,
        new Object[] {badPath, newUsername, newGroup});
  }

  @Test
  public void testProxyGetStats() throws Exception {

    long[] combinedData = routerProtocol.getStats();

    long[] individualData = new long[10];
    for (String nameservice : cluster.getNameservices()) {
      NamenodeContext n = cluster.getNamenode(nameservice, null);
      DFSClient client = n.getClient();
      ClientProtocol clientProtocol = client.getNamenode();
      long[] data = clientProtocol.getStats();
      for (int i = 0; i < data.length; i++) {
        individualData[i] += data[i];
      }
      assert(data.length == combinedData.length);
    }

    for (int i = 0; i < combinedData.length && i < individualData.length; i++) {
      if (i == ClientProtocol.GET_STATS_REMAINING_IDX) {
        // Skip available storage as this fluctuates in mini cluster
        continue;
      }
      assertEquals(combinedData[i], individualData[i]);
    }
  }

  @Test
  public void testProxyGetDatanodeReport() throws Exception {

    DatanodeInfo[] combinedData =
        routerProtocol.getDatanodeReport(DatanodeReportType.ALL);

    Set<Integer> individualData = new HashSet<Integer>();
    for (String nameservice : cluster.getNameservices()) {
      NamenodeContext n = cluster.getNamenode(nameservice, null);
      DFSClient client = n.getClient();
      ClientProtocol clientProtocol = client.getNamenode();
      DatanodeInfo[] data =
          clientProtocol.getDatanodeReport(DatanodeReportType.ALL);
      for (int i = 0; i < data.length; i++) {
        // Collect unique DNs based on their xfer port
        DatanodeInfo info = data[i];
        individualData.add(info.getXferPort());
      }
    }
    assertEquals(combinedData.length, individualData.size());
  }

  @Test
  public void testProxyGetDatanodeStorageReport()
      throws IOException, InterruptedException, URISyntaxException {

    DatanodeStorageReport[] combinedData =
        routerProtocol.getDatanodeStorageReport(DatanodeReportType.ALL);

    Set<String> individualData = new HashSet<>();
    for (String nameservice : cluster.getNameservices()) {
      NamenodeContext n = cluster.getNamenode(nameservice, null);
      DFSClient client = n.getClient();
      ClientProtocol clientProtocol = client.getNamenode();
      DatanodeStorageReport[] data =
          clientProtocol.getDatanodeStorageReport(DatanodeReportType.ALL);
      for (DatanodeStorageReport report : data) {
        // Determine unique DN instances
        DatanodeInfo dn = report.getDatanodeInfo();
        individualData.add(dn.toString());
      }
    }
    assertEquals(combinedData.length, individualData.size());
  }

  @Test
  public void testProxyMkdir() throws Exception {

    // Check the initial folders
    FileStatus[] filesInitial = routerFS.listStatus(new Path("/"));

    // Create a directory via the router at the root level
    String dirPath = "/testdir";
    FsPermission permission = new FsPermission("705");
    routerProtocol.mkdirs(dirPath, permission, false);

    // Verify the root listing has the item via the router
    FileStatus[] files = routerFS.listStatus(new Path("/"));
    assertEquals(Arrays.toString(files) + " should be " +
        Arrays.toString(filesInitial) + " + " + dirPath,
        filesInitial.length + 1, files.length);
    assertTrue(verifyFileExists(routerFS, dirPath));

    // Verify the directory is present in only 1 Namenode
    int foundCount = 0;
    for (NamenodeContext n : cluster.getNamenodes()) {
      if (verifyFileExists(n.getFileSystem(), dirPath)) {
        foundCount++;
      }
    }
    assertEquals(1, foundCount);
    assertTrue(deleteFile(routerFS, dirPath));

    // Validate router failure response matches NN failure response.
    Method m = ClientProtocol.class.getMethod("mkdirs", String.class,
        FsPermission.class, boolean.class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(routerProtocol, nnProtocol, m,
        new Object[] {badPath, permission, false});
  }

  @Test
  public void testProxyChmodFiles() throws Exception {

    FsPermission permission = new FsPermission("444");

    // change permissions
    routerProtocol.setPermission(routerFile, permission);

    // Validate permissions NN
    FileStatus file = getFileStatus(namenode.getFileSystem(), nnFile);
    assertEquals(permission, file.getPermission());

    // Validate router failure response matches NN failure response.
    Method m = ClientProtocol.class.getMethod(
        "setPermission", String.class, FsPermission.class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(routerProtocol, nnProtocol, m,
        new Object[] {badPath, permission});
  }

  @Test
  public void testProxySetReplication() throws Exception {

    // Check current replication via NN
    FileStatus file = getFileStatus(nnFS, nnFile);
    assertEquals(1, file.getReplication());

    // increment replication via router
    routerProtocol.setReplication(routerFile, (short) 2);

    // Verify via NN
    file = getFileStatus(nnFS, nnFile);
    assertEquals(2, file.getReplication());

    // Validate router failure response matches NN failure response.
    Method m = ClientProtocol.class.getMethod(
        "setReplication", String.class, short.class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(routerProtocol, nnProtocol, m,
        new Object[] {badPath, (short) 2});
  }

  @Test
  public void testProxyTruncateFile() throws Exception {

    // Check file size via NN
    FileStatus file = getFileStatus(nnFS, nnFile);
    assertTrue(file.getLen() > 0);

    // Truncate to 0 bytes via router
    routerProtocol.truncate(routerFile, 0, "testclient");

    // Verify via NN
    file = getFileStatus(nnFS, nnFile);
    assertEquals(0, file.getLen());

    // Validate router failure response matches NN failure response.
    Method m = ClientProtocol.class.getMethod(
        "truncate", String.class, long.class, String.class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(routerProtocol, nnProtocol, m,
        new Object[] {badPath, (long) 0, "testclient"});
  }

  @Test
  public void testProxyGetBlockLocations() throws Exception {

    // Fetch block locations via router
    LocatedBlocks locations =
        routerProtocol.getBlockLocations(routerFile, 0, 1024);
    assertEquals(1, locations.getLocatedBlocks().size());

    // Validate router failure response matches NN failure response.
    Method m = ClientProtocol.class.getMethod(
        "getBlockLocations", String.class, long.class, long.class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(routerProtocol, nnProtocol,
        m, new Object[] {badPath, (long) 0, (long) 0});
  }

  @Test
  public void testProxyStoragePolicy() throws Exception {

    // Query initial policy via NN
    HdfsFileStatus status = namenode.getClient().getFileInfo(nnFile);

    // Set a random policy via router
    BlockStoragePolicy[] policies = namenode.getClient().getStoragePolicies();
    BlockStoragePolicy policy = policies[0];

    while (policy.isCopyOnCreateFile()) {
      // Pick a non copy on create policy
      Random rand = new Random();
      int randIndex = rand.nextInt(policies.length);
      policy = policies[randIndex];
    }
    routerProtocol.setStoragePolicy(routerFile, policy.getName());

    // Verify policy via NN
    HdfsFileStatus newStatus = namenode.getClient().getFileInfo(nnFile);
    assertTrue(newStatus.getStoragePolicy() == policy.getId());
    assertTrue(newStatus.getStoragePolicy() != status.getStoragePolicy());

    // Validate router failure response matches NN failure response.
    Method m = ClientProtocol.class.getMethod("setStoragePolicy", String.class,
        String.class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(routerProtocol, nnProtocol,
        m, new Object[] {badPath, "badpolicy"});
  }

  @Test
  public void testProxyGetPreferedBlockSize() throws Exception {

    // Query via NN and Router and verify
    long namenodeSize = nnProtocol.getPreferredBlockSize(nnFile);
    long routerSize = routerProtocol.getPreferredBlockSize(routerFile);
    assertEquals(routerSize, namenodeSize);

    // Validate router failure response matches NN failure response.
    Method m = ClientProtocol.class.getMethod(
        "getPreferredBlockSize", String.class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(
        routerProtocol, nnProtocol, m, new Object[] {badPath});
  }

  private void testConcat(
      String source, String target, boolean failureExpected) {
    boolean failure = false;
    try {
      // Concat test file with fill block length file via router
      routerProtocol.concat(target, new String[] {source});
    } catch (IOException ex) {
      failure = true;
    }
    assertEquals(failureExpected, failure);
  }

  @Test
  public void testProxyConcatFile() throws Exception {

    // Create a stub file in the primary ns
    String sameNameservice = ns;
    String existingFile =
        cluster.getFederatedTestDirectoryForNS(sameNameservice) +
        "_concatfile";
    int existingFileSize = 32;
    createFile(routerFS, existingFile, existingFileSize);

    // Identify an alternate nameservice that doesn't match the existing file
    String alternateNameservice = null;
    for (String n : cluster.getNameservices()) {
      if (!n.equals(sameNameservice)) {
        alternateNameservice = n;
        break;
      }
    }

    // Create new files, must be a full block to use concat. One file is in the
    // same namespace as the target file, the other is in a different namespace.
    String altRouterFile =
        cluster.getFederatedTestDirectoryForNS(alternateNameservice) +
        "_newfile";
    String sameRouterFile =
        cluster.getFederatedTestDirectoryForNS(sameNameservice) +
        "_newfile";
    createFile(routerFS, altRouterFile, DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
    createFile(routerFS, sameRouterFile, DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);

    // Concat in different namespaces, fails
    testConcat(existingFile, altRouterFile, true);

    // Concat in same namespaces, succeeds
    testConcat(existingFile, sameRouterFile, false);

    // Check target file length
    FileStatus status = getFileStatus(routerFS, sameRouterFile);
    assertEquals(
        existingFileSize + DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT,
        status.getLen());

    // Validate router failure response matches NN failure response.
    Method m = ClientProtocol.class.getMethod(
        "concat", String.class, String[].class);
    String badPath = "/unknownlocation/unknowndir";
    compareResponses(routerProtocol, nnProtocol, m,
        new Object[] {badPath, new String[] {routerFile}});
  }

  @Test
  public void testProxyAppend() throws Exception {

    // Append a test string via router
    EnumSet<CreateFlag> createFlag = EnumSet.of(CreateFlag.APPEND);
    DFSClient routerClient = getRouterContext().getClient();
    HdfsDataOutputStream stream =
        routerClient.append(routerFile, 1024, createFlag, null, null);
    stream.writeBytes(TEST_STRING);
    stream.close();

    // Verify file size via NN
    FileStatus status = getFileStatus(nnFS, nnFile);
    assertTrue(status.getLen() > TEST_STRING.length());

    // Validate router failure response matches NN failure response.
    Method m = ClientProtocol.class.getMethod("append", String.class,
        String.class, EnumSetWritable.class);
    String badPath = "/unknownlocation/unknowndir";
    EnumSetWritable<CreateFlag> createFlagWritable =
        new EnumSetWritable<CreateFlag>(createFlag);
    compareResponses(routerProtocol, nnProtocol, m,
        new Object[] {badPath, "testClient", createFlagWritable});
  }

  @Test
  public void testProxyGetAdditionalDatanode()
      throws IOException, InterruptedException, URISyntaxException {

    // Use primitive APIs to open a file, add a block, and get datanode location
    EnumSet<CreateFlag> createFlag = EnumSet.of(CreateFlag.CREATE);
    String clientName = getRouterContext().getClient().getClientName();
    String newRouterFile = routerFile + "_additionalDatanode";
    HdfsFileStatus status = routerProtocol.create(
        newRouterFile, new FsPermission("777"), clientName,
        new EnumSetWritable<CreateFlag>(createFlag), true, (short) 1,
        (long) 1024, CryptoProtocolVersion.supported(), null);

    // Add a block via router (requires client to have same lease)
    LocatedBlock block = routerProtocol.addBlock(
        newRouterFile, clientName, null, null,
        status.getFileId(), null, null);

    DatanodeInfo[] exclusions = new DatanodeInfo[0];
    LocatedBlock newBlock = routerProtocol.getAdditionalDatanode(
        newRouterFile, status.getFileId(), block.getBlock(),
        block.getLocations(), block.getStorageIDs(), exclusions, 1, clientName);
    assertNotNull(newBlock);
  }

  @Test
  public void testProxyCreateFileAlternateUser()
      throws IOException, URISyntaxException, InterruptedException {

    // Create via Router
    String routerDir = cluster.getFederatedTestDirectoryForNS(ns);
    String namenodeDir = cluster.getNamenodeTestDirectoryForNS(ns);
    String newRouterFile = routerDir + "/unknownuser";
    String newNamenodeFile = namenodeDir + "/unknownuser";
    String username = "unknownuser";

    // Allow all user access to dir
    namenode.getFileContext().setPermission(
        new Path(namenodeDir), new FsPermission("777"));

    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(username);
    DFSClient client = getRouterContext().getClient(ugi);
    client.create(newRouterFile, true);

    // Fetch via NN and check user
    FileStatus status = getFileStatus(nnFS, newNamenodeFile);
    assertEquals(status.getOwner(), username);
  }

  @Test
  public void testProxyGetFileInfoAcessException() throws IOException {

    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser("unknownuser");

    // List files from the NN and trap the exception
    Exception nnFailure = null;
    try {
      String testFile = cluster.getNamenodeTestFileForNS(ns);
      namenode.getClient(ugi).getLocatedBlocks(testFile, 0);
    } catch (Exception e) {
      nnFailure = e;
    }
    assertNotNull(nnFailure);

    // List files from the router and trap the exception
    Exception routerFailure = null;
    try {
      String testFile = cluster.getFederatedTestFileForNS(ns);
      getRouterContext().getClient(ugi).getLocatedBlocks(testFile, 0);
    } catch (Exception e) {
      routerFailure = e;
    }
    assertNotNull(routerFailure);

    assertEquals(routerFailure.getClass(), nnFailure.getClass());
  }
}