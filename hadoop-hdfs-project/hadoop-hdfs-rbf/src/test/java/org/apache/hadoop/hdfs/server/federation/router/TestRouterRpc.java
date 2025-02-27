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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_MAX_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_WITH_REMOTE_PORT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.addDirectory;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.countContents;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createFile;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.deleteFile;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.getFileStatus;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.verifyFileExists;
import static org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.TEST_STRING;
import static org.apache.hadoop.ipc.CallerContext.PROXY_USER_PORT;
import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ECBlockGroupStats;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyState;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.ReplicatedBlockStats;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCMetrics;
import org.apache.hadoop.hdfs.server.federation.metrics.NamenodeBeanMetrics;
import org.apache.hadoop.hdfs.server.federation.metrics.RBFMetrics;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

/**
 * The the RPC interface of the {@link Router} implemented by
 * {@link RouterRpcServer}.
 * Tests covering the functionality of RouterRPCServer with
 * multi nameServices.
 */
public class TestRouterRpc {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterRpc.class);

  protected static final int NUM_SUBCLUSTERS = 2;
  // We need at least 6 DNs to test Erasure Coding with RS-6-3-64k
  protected static final int NUM_DNS = 6;


  protected static final Comparator<ErasureCodingPolicyInfo> EC_POLICY_CMP =
      new Comparator<ErasureCodingPolicyInfo>() {
        public int compare(
            ErasureCodingPolicyInfo ec0,
            ErasureCodingPolicyInfo ec1) {
          String name0 = ec0.getPolicy().getName();
          String name1 = ec1.getPolicy().getName();
          return name0.compareTo(name1);
        }
      };

  /** Federated HDFS cluster. */
  private static MiniRouterDFSCluster cluster;

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

  /** NameNodeProtocol interface to the Router. */
  private NamenodeProtocol routerNamenodeProtocol;
  /** NameNodeProtocol interface to the Namenode. */
  private NamenodeProtocol nnNamenodeProtocol;
  private NamenodeProtocol nnNamenodeProtocol1;

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
    // Start routers with only an RPC service
    Configuration routerConf = new RouterConfigBuilder()
        .metrics()
        .rpc()
        .build();
    // We decrease the DN cache times to make the test faster
    routerConf.setTimeDuration(
        RBFConfigKeys.DN_REPORT_CACHE_EXPIRE, 1, TimeUnit.SECONDS);
    setUp(routerConf);
  }

  public static void setUp(Configuration routerConf) throws Exception {
    Configuration namenodeConf = new Configuration();
    namenodeConf.setBoolean(DFSConfigKeys.HADOOP_CALLER_CONTEXT_ENABLED_KEY,
        true);
    namenodeConf.set(HADOOP_CALLER_CONTEXT_MAX_SIZE_KEY, "256");
    // It's very easy to become overloaded for some specific dn in this small
    // cluster, which will cause the EC file block allocation failure. To avoid
    // this issue, we disable considerLoad option.
    namenodeConf.setBoolean(DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY, false);
    namenodeConf.setBoolean(DFS_NAMENODE_AUDIT_LOG_WITH_REMOTE_PORT_KEY, true);
    cluster = new MiniRouterDFSCluster(false, NUM_SUBCLUSTERS);
    cluster.setNumDatanodesPerNameservice(NUM_DNS);
    cluster.addNamenodeOverrides(namenodeConf);
    cluster.setIndependentDNs();

    Configuration conf = new Configuration();
    // Setup proxy users.
    conf.set("hadoop.proxyuser.testRealUser.groups", "*");
    conf.set("hadoop.proxyuser.testRealUser.hosts", "*");
    String loginUser = UserGroupInformation.getLoginUser().getUserName();
    conf.set(String.format("hadoop.proxyuser.%s.groups", loginUser), "*");
    conf.set(String.format("hadoop.proxyuser.%s.hosts", loginUser), "*");
    // Enable IP proxy users.
    conf.set(DFSConfigKeys.DFS_NAMENODE_IP_PROXY_USERS, "placeholder");
    conf.setInt(DFSConfigKeys.DFS_LIST_LIMIT, 5);
    cluster.addNamenodeOverrides(conf);
    // Start NNs and DNs and wait until ready
    cluster.startCluster();

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
  }

  @After
  public void cleanup() {
    // clear client context
    CallerContext.setCurrent(null);
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
    RouterContext rndRouter = cluster.getRandomRouter();
    this.setRouter(rndRouter);

    // Pick a namenode for this test
    String ns0 = cluster.getNameservices().get(0);
    this.setNs(ns0);
    this.setNamenode(cluster.getNamenode(ns0, null));

    // Create a test file on the NN
    Random rnd = new Random();
    String randomFile = "testfile-" + rnd.nextInt();
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

  protected MiniRouterDFSCluster getCluster() {
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
    this.routerNamenodeProtocol = NameNodeProxies.createProxy(router.getConf(),
        router.getFileSystem().getUri(), NamenodeProtocol.class).getProxy();
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

    // Namenode from the default namespace
    String ns0 = cluster.getNameservices().get(0);
    NamenodeContext nn0 = cluster.getNamenode(ns0, null);
    this.nnNamenodeProtocol = NameNodeProxies.createProxy(nn0.getConf(),
        nn0.getFileSystem().getUri(), NamenodeProtocol.class).getProxy();
    // Namenode from the other namespace
    String ns1 = cluster.getNameservices().get(1);
    NamenodeContext nn1 = cluster.getNamenode(ns1, null);
    this.nnNamenodeProtocol1 = NameNodeProxies.createProxy(nn1.getConf(),
        nn1.getFileSystem().getUri(), NamenodeProtocol.class).getProxy();
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
  public void testProxyListFilesLargeDir() throws IOException {
    // Call listStatus against a dir with many files
    // Create a parent point as well as a subfolder mount
    // /parent
    //    ns0 -> /parent
    // /parent/file-7
    //    ns0 -> /parent/file-7
    // /parent/file-0
    //    ns0 -> /parent/file-0
    for (RouterContext rc : cluster.getRouters()) {
      MockResolver resolver =
          (MockResolver) rc.getRouter().getSubclusterResolver();
      resolver.addLocation("/parent", ns, "/parent");
      // file-0 is only in mount table
      resolver.addLocation("/parent/file-0", ns, "/parent/file-0");
      // file-7 is both in mount table and in file system
      resolver.addLocation("/parent/file-7", ns, "/parent/file-7");
    }

    // Test the case when there is no subcluster path and only mount point
    FileStatus[] result = routerFS.listStatus(new Path("/parent"));
    assertEquals(2, result.length);
    // this makes sure file[0-8] is added in order
    assertEquals("file-0", result[0].getPath().getName());
    assertEquals("file-7", result[1].getPath().getName());

    // Create files and test full listing in order
    NamenodeContext nn = cluster.getNamenode(ns, null);
    FileSystem nnFileSystem = nn.getFileSystem();
    for (int i = 1; i < 9; i++) {
      createFile(nnFileSystem, "/parent/file-"+i, 32);
    }

    result = routerFS.listStatus(new Path("/parent"));
    assertEquals(9, result.length);
    // this makes sure file[0-8] is added in order
    for (int i = 0; i < 9; i++) {
      assertEquals("file-"+i, result[i].getPath().getName());
    }

    // Add file-9 and now this listing will be added from mount point
    for (RouterContext rc : cluster.getRouters()) {
      MockResolver resolver =
          (MockResolver) rc.getRouter().getSubclusterResolver();
      resolver.addLocation("/parent/file-9", ns, "/parent/file-9");
    }
    assertFalse(verifyFileExists(nnFileSystem, "/parent/file-9"));
    result = routerFS.listStatus(new Path("/parent"));
    // file-9 will be added by mount point
    assertEquals(10, result.length);
    for (int i = 0; i < 10; i++) {
      assertEquals("file-"+i, result[i].getPath().getName());
    }
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
    // Some of the statistics are out of sync because of the mini cluster
    Supplier<Boolean> check = new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          long[] combinedData = routerProtocol.getStats();
          long[] individualData = getAggregateStats();
          int len = Math.min(combinedData.length, individualData.length);
          for (int i = 0; i < len; i++) {
            if (combinedData[i] != individualData[i]) {
              LOG.error("Stats for {} don't match: {} != {}",
                  i, combinedData[i], individualData[i]);
              return false;
            }
          }
          return true;
        } catch (Exception e) {
          LOG.error("Cannot get stats: {}", e.getMessage());
          return false;
        }
      }
    };
    GenericTestUtils.waitFor(check, 500, 5 * 1000);
  }

  /**
   * Get the sum of each subcluster statistics.
   * @return Aggregated statistics.
   * @throws Exception If it cannot get the stats from the Router or Namenode.
   */
  private long[] getAggregateStats() throws Exception {
    long[] individualData = new long[10];
    for (String nameservice : cluster.getNameservices()) {
      NamenodeContext n = cluster.getNamenode(nameservice, null);
      DFSClient client = n.getClient();
      ClientProtocol clientProtocol = client.getNamenode();
      long[] data = clientProtocol.getStats();
      for (int i = 0; i < data.length; i++) {
        individualData[i] += data[i];
      }
    }
    return individualData;
  }

  @Test
  public void testProxyGetDatanodeReport() throws Exception {

    DatanodeInfo[] combinedData =
        routerProtocol.getDatanodeReport(DatanodeReportType.ALL);
    assertEquals(0, routerProtocol.getSlowDatanodeReport().length);
    final Map<Integer, String> routerDNMap = new TreeMap<>();
    for (DatanodeInfo dn : combinedData) {
      String subcluster = dn.getNetworkLocation().split("/")[1];
      routerDNMap.put(dn.getXferPort(), subcluster);
    }

    final Map<Integer, String> nnDNMap = new TreeMap<>();
    for (String nameservice : cluster.getNameservices()) {
      NamenodeContext n = cluster.getNamenode(nameservice, null);
      DFSClient client = n.getClient();
      ClientProtocol clientProtocol = client.getNamenode();
      DatanodeInfo[] data =
          clientProtocol.getDatanodeReport(DatanodeReportType.ALL);
      for (int i = 0; i < data.length; i++) {
        // Collect unique DNs based on their xfer port
        DatanodeInfo info = data[i];
        nnDNMap.put(info.getXferPort(), nameservice);
      }
    }
    assertEquals(nnDNMap, routerDNMap);
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
  public void testAllowDisallowSnapshots() throws Exception {

    // Create a directory via the router at the root level
    String dirPath = "/testdir";
    String filePath1 = "/sample";
    FsPermission permission = new FsPermission("705");
    routerProtocol.mkdirs(dirPath, permission, false);
    createFile(routerFS, filePath1, 32);

    // Check that initially doesn't allow snapshots
    NamenodeContext nnContext = cluster.getNamenodes().get(0);
    NameNode nn = nnContext.getNamenode();
    FSNamesystem fsn = NameNodeAdapter.getNamesystem(nn);
    FSDirectory fsdir = fsn.getFSDirectory();
    INodeDirectory dirNode = fsdir.getINode4Write(dirPath).asDirectory();
    assertFalse(dirNode.isSnapshottable());

    // Allow snapshots and verify the folder allows them
    routerProtocol.allowSnapshot("/testdir");
    dirNode = fsdir.getINode4Write(dirPath).asDirectory();
    assertTrue(dirNode.isSnapshottable());

    // Disallow snapshot on dir and verify does not allow snapshots anymore
    routerProtocol.disallowSnapshot("/testdir");
    dirNode = fsdir.getINode4Write(dirPath).asDirectory();
    assertFalse(dirNode.isSnapshottable());

    // Cleanup
    routerProtocol.delete(dirPath, true);
  }

  @Test
  public void testManageSnapshot() throws Exception {

    final String mountPoint = "/mntsnapshot";
    final String snapshotFolder = mountPoint + "/folder";
    LOG.info("Setup a mount point for snapshots: {}", mountPoint);
    Router r = router.getRouter();
    MockResolver resolver = (MockResolver) r.getSubclusterResolver();
    String ns0 = cluster.getNameservices().get(0);
    resolver.addLocation(mountPoint, ns0, "/");

    FsPermission permission = new FsPermission("777");
    routerProtocol.mkdirs(snapshotFolder, permission, false);
    try {
      for (int i = 1; i <= 9; i++) {
        String folderPath = snapshotFolder + "/subfolder" + i;
        routerProtocol.mkdirs(folderPath, permission, false);
      }

      LOG.info("Create the snapshot: {}", snapshotFolder);
      routerProtocol.allowSnapshot(snapshotFolder);
      String snapshotName =
          routerProtocol.createSnapshot(snapshotFolder, "snap");
      assertEquals(snapshotFolder + "/.snapshot/snap", snapshotName);
      assertTrue(
          verifyFileExists(routerFS, snapshotFolder + "/.snapshot/snap"));

      LOG.info("Rename the snapshot and check it changed");
      routerProtocol.renameSnapshot(snapshotFolder, "snap", "newsnap");
      assertFalse(
          verifyFileExists(routerFS, snapshotFolder + "/.snapshot/snap"));
      assertTrue(
          verifyFileExists(routerFS, snapshotFolder + "/.snapshot/newsnap"));
      LambdaTestUtils.intercept(SnapshotException.class,
          "Cannot delete snapshot snap from path " + snapshotFolder + ":",
          () -> routerFS.deleteSnapshot(new Path(snapshotFolder), "snap"));

      LOG.info("Delete the snapshot and check it is not there");
      routerProtocol.deleteSnapshot(snapshotFolder, "newsnap");
      assertFalse(
          verifyFileExists(routerFS, snapshotFolder + "/.snapshot/newsnap"));
    } finally {
      // Cleanup
      assertTrue(routerProtocol.delete(snapshotFolder, true));
      assertTrue(resolver.removeLocation(mountPoint, ns0, "/"));
    }
  }

  @Test
  public void testGetSnapshotListing() throws IOException {

    // Create a directory via the router and allow snapshots
    final String snapshotPath = "/testGetSnapshotListing";
    final String childDir = snapshotPath + "/subdir";
    FsPermission permission = new FsPermission("705");
    routerProtocol.mkdirs(snapshotPath, permission, false);
    routerProtocol.allowSnapshot(snapshotPath);

    // Create two snapshots
    final String snapshot1 = "snap1";
    final String snapshot2 = "snap2";
    routerProtocol.createSnapshot(snapshotPath, snapshot1);
    routerProtocol.mkdirs(childDir, permission, false);
    routerProtocol.createSnapshot(snapshotPath, snapshot2);

    // Check for listing through the Router
    SnapshottableDirectoryStatus[] dirList =
        routerProtocol.getSnapshottableDirListing();
    assertEquals(1, dirList.length);
    SnapshottableDirectoryStatus snapshotDir0 = dirList[0];
    assertEquals(snapshotPath, snapshotDir0.getFullPath().toString());

    // check for snapshot listing through the Router
    SnapshotStatus[] snapshots = routerProtocol.
        getSnapshotListing(snapshotPath);
    assertEquals(2, snapshots.length);
    assertEquals(SnapshotTestHelper.getSnapshotRoot(
        new Path(snapshotPath), snapshot1),
        snapshots[0].getFullPath());
    assertEquals(SnapshotTestHelper.getSnapshotRoot(
        new Path(snapshotPath), snapshot2),
        snapshots[1].getFullPath());
    // Check for difference report in two snapshot
    SnapshotDiffReport diffReport = routerProtocol.getSnapshotDiffReport(
        snapshotPath, snapshot1, snapshot2);
    assertEquals(2, diffReport.getDiffList().size());

    // Check for difference in two snapshot
    byte[] startPath = {};
    SnapshotDiffReportListing diffReportListing =
        routerProtocol.getSnapshotDiffReportListing(
            snapshotPath, snapshot1, snapshot2, startPath, -1);
    assertEquals(1, diffReportListing.getModifyList().size());
    assertEquals(1, diffReportListing.getCreateList().size());

    // Cleanup
    routerProtocol.deleteSnapshot(snapshotPath, snapshot1);
    routerProtocol.deleteSnapshot(snapshotPath, snapshot2);
    routerProtocol.disallowSnapshot(snapshotPath);
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
  public void testProxyGetAndUnsetStoragePolicy() throws Exception {
    String file = "/testGetStoragePolicy";
    String nnFilePath = cluster.getNamenodeTestDirectoryForNS(ns) + file;
    String routerFilePath = cluster.getFederatedTestDirectoryForNS(ns) + file;

    createFile(routerFS, routerFilePath, 32);

    // Get storage policy via router
    BlockStoragePolicy policy = routerProtocol.getStoragePolicy(routerFilePath);
    // Verify default policy is HOT
    assertEquals(HdfsConstants.HOT_STORAGE_POLICY_NAME, policy.getName());
    assertEquals(HdfsConstants.HOT_STORAGE_POLICY_ID, policy.getId());

    // Get storage policies via router
    BlockStoragePolicy[] policies = routerProtocol.getStoragePolicies();
    BlockStoragePolicy[] nnPolicies = namenode.getClient().getStoragePolicies();
    // Verify policie returned by router is same as policies returned by NN
    assertArrayEquals(nnPolicies, policies);

    BlockStoragePolicy newPolicy = policies[0];
    while (newPolicy.isCopyOnCreateFile()) {
      // Pick a non copy on create policy. Beacuse if copyOnCreateFile is set
      // then the policy cannot be changed after file creation.
      Random rand = new Random();
      int randIndex = rand.nextInt(policies.length);
      newPolicy = policies[randIndex];
    }
    routerProtocol.setStoragePolicy(routerFilePath, newPolicy.getName());

    // Get storage policy via router
    policy = routerProtocol.getStoragePolicy(routerFilePath);
    // Verify default policy
    assertEquals(newPolicy.getName(), policy.getName());
    assertEquals(newPolicy.getId(), policy.getId());

    // Verify policy via NN
    BlockStoragePolicy nnPolicy =
        namenode.getClient().getStoragePolicy(nnFilePath);
    assertEquals(nnPolicy.getName(), policy.getName());
    assertEquals(nnPolicy.getId(), policy.getId());

    // Unset storage policy via router
    routerProtocol.unsetStoragePolicy(routerFilePath);

    // Get storage policy
    policy = routerProtocol.getStoragePolicy(routerFilePath);
    assertEquals(HdfsConstants.HOT_STORAGE_POLICY_NAME, policy.getName());
    assertEquals(HdfsConstants.HOT_STORAGE_POLICY_ID, policy.getId());

    // Verify policy via NN
    nnPolicy = namenode.getClient().getStoragePolicy(nnFilePath);
    assertEquals(nnPolicy.getName(), policy.getName());
    assertEquals(nnPolicy.getId(), policy.getId());
  }

  @Test
  public void testListStoragePolicies() throws IOException, URISyntaxException {
    MockResolver resolver =
        (MockResolver) router.getRouter().getSubclusterResolver();
    try {
      // Check with default namespace specified.
      BlockStoragePolicy[] policies = namenode.getClient().getStoragePolicies();
      assertArrayEquals(policies, routerProtocol.getStoragePolicies());
      // Check with default namespace unspecified.
      resolver.setDisableNamespace(true);
      assertArrayEquals(policies, routerProtocol.getStoragePolicies());
    } finally {
      resolver.setDisableNamespace(false);
    }
  }

  @Test
  public void testGetServerDefaults() throws IOException, URISyntaxException {
    MockResolver resolver =
        (MockResolver) router.getRouter().getSubclusterResolver();
    try {
      // Check with default namespace specified.
      FsServerDefaults defaults = namenode.getClient().getServerDefaults();
      assertEquals(defaults.getBlockSize(),
          routerProtocol.getServerDefaults().getBlockSize());
      // Check with default namespace unspecified.
      resolver.setDisableNamespace(true);
      assertEquals(defaults.getBlockSize(),
          routerProtocol.getServerDefaults().getBlockSize());
    } finally {
      resolver.setDisableNamespace(false);
    }
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
      String source, String target, boolean failureExpected, boolean verfiyException, String msg) {
    boolean failure = false;
    try {
      // Concat test file with fill block length file via router
      routerProtocol.concat(target, new String[] {source});
    } catch (IOException ex) {
      failure = true;
      if (verfiyException) {
        assertExceptionContains(msg, ex);
      }
    }
    assertEquals(failureExpected, failure);
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

    // Test when concat trg is a empty file
    createFile(routerFS, existingFile, existingFileSize);
    String sameRouterEmptyFile =
        cluster.getFederatedTestDirectoryForNS(sameNameservice) +
            "_newemptyfile";
    createFile(routerFS, sameRouterEmptyFile, 0);
    // Concat in same namespaces, succeeds
    testConcat(existingFile, sameRouterEmptyFile, false);
    FileStatus mergedStatus = getFileStatus(routerFS, sameRouterEmptyFile);
    assertEquals(existingFileSize, mergedStatus.getLen());

    // Test when concat srclist has some empty file, namenode will throw IOException.
    String srcEmptyFile = cluster.getFederatedTestDirectoryForNS(sameNameservice) + "_srcEmptyFile";
    createFile(routerFS, srcEmptyFile, 0);
    String targetFile = cluster.getFederatedTestDirectoryForNS(sameNameservice) + "_targetFile";
    createFile(routerFS, targetFile, existingFileSize);
    // Concat in same namespaces, succeeds
    testConcat(srcEmptyFile, targetFile, true, true,
        "org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.HadoopIllegalArgumentException): "
            + "concat: source file " + srcEmptyFile + " is invalid or empty or underConstruction");
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
        (long) 1024, CryptoProtocolVersion.supported(), null, null);

    // Add a block via router (requires client to have same lease)
    LocatedBlock block = routerProtocol.addBlock(
        newRouterFile, clientName, null, null,
        status.getFileId(), null, null);

    DatanodeInfo[] exclusions = DatanodeInfo.EMPTY_ARRAY;
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

  @Test
  public void testProxyVersionRequest() throws Exception {
    MockResolver resolver =
        (MockResolver) router.getRouter().getSubclusterResolver();
    try {
      // Check with default namespace specified.
      NamespaceInfo rVersion = routerNamenodeProtocol.versionRequest();
      NamespaceInfo nnVersion = nnNamenodeProtocol.versionRequest();
      NamespaceInfo nnVersion1 = nnNamenodeProtocol1.versionRequest();
      compareVersion(rVersion, nnVersion);
      // Check with default namespace unspecified.
      resolver.setDisableNamespace(true);
      // Verify the NamespaceInfo is of nn0 or nn1
      boolean isNN0 =
          rVersion.getBlockPoolID().equals(nnVersion.getBlockPoolID());
      compareVersion(rVersion, isNN0 ? nnVersion : nnVersion1);
    } finally {
      resolver.setDisableNamespace(false);
    }
  }

  private void compareVersion(NamespaceInfo rVersion, NamespaceInfo nnVersion) {
    assertEquals(nnVersion.getBlockPoolID(), rVersion.getBlockPoolID());
    assertEquals(nnVersion.getNamespaceID(), rVersion.getNamespaceID());
    assertEquals(nnVersion.getClusterID(), rVersion.getClusterID());
    assertEquals(nnVersion.getLayoutVersion(), rVersion.getLayoutVersion());
    assertEquals(nnVersion.getCTime(), rVersion.getCTime());
  }

  @Test
  public void testProxyGetBlockKeys() throws Exception {
    MockResolver resolver =
        (MockResolver) router.getRouter().getSubclusterResolver();
    try {
      // Check with default namespace specified.
      ExportedBlockKeys rKeys = routerNamenodeProtocol.getBlockKeys();
      ExportedBlockKeys nnKeys = nnNamenodeProtocol.getBlockKeys();
      compareBlockKeys(rKeys, nnKeys);
      // Check with default namespace unspecified.
      resolver.setDisableNamespace(true);
      rKeys = routerNamenodeProtocol.getBlockKeys();
      compareBlockKeys(rKeys, nnKeys);
    } finally {
      resolver.setDisableNamespace(false);
    }
  }

  private void compareBlockKeys(ExportedBlockKeys rKeys,
      ExportedBlockKeys nnKeys) {
    assertEquals(nnKeys.getCurrentKey(), rKeys.getCurrentKey());
    assertEquals(nnKeys.getKeyUpdateInterval(), rKeys.getKeyUpdateInterval());
    assertEquals(nnKeys.getTokenLifetime(), rKeys.getTokenLifetime());
  }

  @Test
  public void testProxyGetBlocks() throws Exception {
    // Get datanodes
    DatanodeInfo[] dns =
        routerProtocol.getDatanodeReport(DatanodeReportType.ALL);
    DatanodeInfo dn0 = dns[0];

    // Verify that checking that datanode works
    BlocksWithLocations routerBlockLocations =
        routerNamenodeProtocol.getBlocks(dn0, 1024, 0, 0,
            null);
    BlocksWithLocations nnBlockLocations =
        nnNamenodeProtocol.getBlocks(dn0, 1024, 0, 0,
            null);
    BlockWithLocations[] routerBlocks = routerBlockLocations.getBlocks();
    BlockWithLocations[] nnBlocks = nnBlockLocations.getBlocks();
    assertEquals(nnBlocks.length, routerBlocks.length);
    for (int i = 0; i < routerBlocks.length; i++) {
      assertEquals(
          nnBlocks[i].getBlock().getBlockId(),
          routerBlocks[i].getBlock().getBlockId());
    }
  }

  @Test
  public void testProxyGetTransactionID() throws IOException {
    MockResolver resolver =
        (MockResolver) router.getRouter().getSubclusterResolver();
    try {
      // Check with default namespace specified.
      long routerTransactionID = routerNamenodeProtocol.getTransactionID();
      long nnTransactionID = nnNamenodeProtocol.getTransactionID();
      long nnTransactionID1 = nnNamenodeProtocol1.getTransactionID();
      assertEquals(nnTransactionID, routerTransactionID);
      // Check with default namespace unspecified.
      resolver.setDisableNamespace(true);
      // Verify the transaction ID is of nn0 or nn1
      routerTransactionID = routerNamenodeProtocol.getTransactionID();
      assertThat(routerTransactionID).isIn(nnTransactionID, nnTransactionID1);
    } finally {
      resolver.setDisableNamespace(false);
    }
  }

  @Test
  public void testProxyGetMostRecentCheckpointTxId() throws IOException {
    MockResolver resolver =
        (MockResolver) router.getRouter().getSubclusterResolver();
    try {
      // Check with default namespace specified.
      long routerCheckPointId =
          routerNamenodeProtocol.getMostRecentCheckpointTxId();
      long nnCheckPointId = nnNamenodeProtocol.getMostRecentCheckpointTxId();
      assertEquals(nnCheckPointId, routerCheckPointId);
      // Check with default namespace unspecified.
      resolver.setDisableNamespace(true);
      routerCheckPointId = routerNamenodeProtocol.getMostRecentCheckpointTxId();
    } finally {
      resolver.setDisableNamespace(false);
    }
  }

  @Test
  public void testProxySetSafemode() throws Exception {
    boolean routerSafemode =
        routerProtocol.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_GET, false);
    boolean nnSafemode =
        nnProtocol.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_GET, false);
    assertEquals(nnSafemode, routerSafemode);

    routerSafemode =
        routerProtocol.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_GET, true);
    nnSafemode =
        nnProtocol.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_GET, true);
    assertEquals(nnSafemode, routerSafemode);

    assertFalse(routerProtocol.setSafeMode(
        HdfsConstants.SafeModeAction.SAFEMODE_GET, false));
    assertTrue(routerProtocol.setSafeMode(
        HdfsConstants.SafeModeAction.SAFEMODE_ENTER, false));
    assertTrue(routerProtocol.setSafeMode(
        HdfsConstants.SafeModeAction.SAFEMODE_GET, false));
    assertFalse(routerProtocol.setSafeMode(
        HdfsConstants.SafeModeAction.SAFEMODE_LEAVE, false));
    assertFalse(routerProtocol.setSafeMode(
        HdfsConstants.SafeModeAction.SAFEMODE_GET, false));
  }

  @Test
  public void testProxyRestoreFailedStorage() throws Exception {
    boolean routerSuccess = routerProtocol.restoreFailedStorage("check");
    boolean nnSuccess = nnProtocol.restoreFailedStorage("check");
    assertEquals(nnSuccess, routerSuccess);
  }

  private void testRenewLeaseInternal(DistributedFileSystem dfs,
      FederationRPCMetrics rpcMetrics, Path testPath, boolean createFlag)
      throws Exception {
    FSDataOutputStream outputStream = null;
    try {
      if (createFlag) {
        outputStream = dfs.create(testPath);
      } else {
        outputStream = dfs.append(testPath);
      }
      outputStream.write("hello world. \n".getBytes());
      long proxyOpBeforeRenewLease = rpcMetrics.getProxyOps();
      assertTrue(dfs.getClient().renewLease());
      long proxyOpAfterRenewLease = rpcMetrics.getProxyOps();
      assertEquals((proxyOpBeforeRenewLease + 1), proxyOpAfterRenewLease);
    } finally {
      if (outputStream != null) {
        outputStream.close();
      }
    }
  }

  @Test
  public void testRenewLeaseForECFile() throws Exception {
    String ecName = "RS-6-3-1024k";
    FederationRPCMetrics metrics = router.getRouterRpcServer().getRPCMetrics();
    // Install a mount point to a different path to check
    MockResolver resolver =
        (MockResolver)router.getRouter().getSubclusterResolver();
    String ns0 = cluster.getNameservices().get(0);
    resolver.addLocation("/testRenewLease0", ns0, "/testRenewLease0");

    // Stop LeaseRenewer
    DistributedFileSystem routerDFS = (DistributedFileSystem) routerFS;
    routerDFS.getClient().getLeaseRenewer().interruptAndJoin();

    Path testECPath = new Path("/testRenewLease0/ecDirectory/test_ec.txt");
    routerDFS.mkdirs(testECPath.getParent());
    routerDFS.setErasureCodingPolicy(
        testECPath.getParent(), ecName);
    testRenewLeaseInternal(routerDFS, metrics, testECPath, true);

    ErasureCodingPolicy ecPolicy = routerDFS.getErasureCodingPolicy(testECPath);
    assertNotNull(ecPolicy);
    assertEquals(ecName, ecPolicy.getName());
  }


  @Test
  public void testRenewLeaseForReplicaFile() throws Exception {
    FederationRPCMetrics metrics = router.getRouterRpcServer().getRPCMetrics();
    // Install a mount point to a different path to check
    MockResolver resolver =
        (MockResolver)router.getRouter().getSubclusterResolver();
    String ns0 = cluster.getNameservices().get(0);
    resolver.addLocation("/testRenewLease0", ns0, "/testRenewLease0");

    // Stop LeaseRenewer
    DistributedFileSystem routerDFS = (DistributedFileSystem) routerFS;
    routerDFS.getClient().getLeaseRenewer().interruptAndJoin();

    // Test Replica File
    Path testPath = new Path("/testRenewLease0/test_replica.txt");
    testRenewLeaseInternal(routerDFS, metrics, testPath, true);
    testRenewLeaseInternal(routerDFS, metrics, testPath, false);
  }

  @Test
  public void testRenewLeaseWithMultiStream() throws Exception {
    FederationRPCMetrics metrics = router.getRouterRpcServer().getRPCMetrics();
    // Install a mount point to a different path to check
    MockResolver resolver =
        (MockResolver)router.getRouter().getSubclusterResolver();
    String ns0 = cluster.getNameservices().get(0);
    String ns1 = cluster.getNameservices().get(1);
    resolver.addLocation("/testRenewLease0", ns0, "/testRenewLease0");
    resolver.addLocation("/testRenewLease1", ns1, "/testRenewLease1");

    // Stop LeaseRenewer
    DistributedFileSystem routerDFS = (DistributedFileSystem) routerFS;
    routerDFS.getClient().getLeaseRenewer().interruptAndJoin();

    Path newTestPath0 = new Path("/testRenewLease0/test1.txt");
    Path newTestPath1 = new Path("/testRenewLease1/test1.txt");
    try (FSDataOutputStream outStream1 = routerDFS.create(newTestPath0);
         FSDataOutputStream outStream2 = routerDFS.create(newTestPath1)) {
      outStream1.write("hello world \n".getBytes());
      outStream2.write("hello world \n".getBytes());
      long proxyOpBeforeRenewLease2 = metrics.getProxyOps();
      assertTrue(routerDFS.getClient().renewLease());
      long proxyOpAfterRenewLease2 = metrics.getProxyOps();
      assertEquals((proxyOpBeforeRenewLease2 + 2), proxyOpAfterRenewLease2);
    }
  }

  @Test
  public void testMkdirWithDisableNameService() throws Exception {
    MockResolver resolver = (MockResolver)router.getRouter().getSubclusterResolver();
    String ns0 = cluster.getNameservices().get(0);
    resolver.addLocation("/mnt", ns0, "/");
    MockResolver activeNamenodeResolver = (MockResolver)router.getRouter().getNamenodeResolver();
    activeNamenodeResolver.disableNamespace(ns0);

    try {
      FsPermission permission = new FsPermission("777");
      RouterRpcServer rpcServer = router.getRouter().getRpcServer();
      LambdaTestUtils.intercept(NoLocationException.class,
          () -> rpcServer.mkdirs("/mnt/folder0/folder1", permission, true));
    } finally {
      activeNamenodeResolver.clearDisableNamespaces();
    }
  }

  @Test
  public void testProxyExceptionMessages() throws IOException {

    // Install a mount point to a different path to check
    MockResolver resolver =
        (MockResolver)router.getRouter().getSubclusterResolver();
    String ns0 = cluster.getNameservices().get(0);
    resolver.addLocation("/mnt", ns0, "/");

    try {
      FsPermission permission = new FsPermission("777");
      routerProtocol.mkdirs("/mnt/folder0/folder1", permission, false);
      fail("mkdirs for non-existing parent folder should have failed");
    } catch (IOException ioe) {
      assertExceptionContains("/mnt/folder0", ioe,
          "Wrong path in exception for mkdirs");
    }

    try {
      FsPermission permission = new FsPermission("777");
      routerProtocol.setPermission("/mnt/testfile.txt", permission);
      fail("setPermission for non-existing file should have failed");
    } catch (IOException ioe) {
      assertExceptionContains("/mnt/testfile.txt", ioe,
          "Wrong path in exception for setPermission");
    }

    try {
      FsPermission permission = new FsPermission("777");
      routerProtocol.mkdirs("/mnt/folder0/folder1", permission, false);
      routerProtocol.delete("/mnt/folder0", false);
      fail("delete for non-existing file should have failed");
    } catch (IOException ioe) {
      assertExceptionContains("/mnt/folder0", ioe,
          "Wrong path in exception for delete");
    }

    resolver.cleanRegistrations();

    // Check corner cases
    assertEquals(
        "Parent directory doesn't exist: /ns1/a/a/b",
        RouterRpcClient.processExceptionMsg(
            "Parent directory doesn't exist: /a/a/b", "/a", "/ns1/a"));
  }

  /**
   * Create a file for each NameSpace, then find their 1st block and mark one of
   * the replica as corrupt through BlockManager#findAndMarkBlockAsCorrupt.
   *
   * After all NameNode received the corrupt replica report, the
   * replicatedBlockStats.getCorruptBlocks() should equal to the sum of
   * corruptBlocks of all NameSpaces.
   */
  @Test
  public void testGetReplicatedBlockStats() throws Exception {
    String testFile = "/test-file";
    for (String nsid : cluster.getNameservices()) {
      NamenodeContext context = cluster.getNamenode(nsid, null);
      NameNode nameNode = context.getNamenode();
      FSNamesystem namesystem = nameNode.getNamesystem();
      BlockManager bm = namesystem.getBlockManager();
      FileSystem fileSystem = context.getFileSystem();

      // create a test file
      createFile(fileSystem, testFile, 1024);
      // mark a replica as corrupt
      LocatedBlock block = NameNodeAdapter
          .getBlockLocations(nameNode, testFile, 0, 1024).get(0);
      namesystem.writeLock(RwLockMode.BM);
      bm.findAndMarkBlockAsCorrupt(block.getBlock(), block.getLocations()[0],
          "STORAGE_ID", "TEST");
      namesystem.writeUnlock(RwLockMode.BM, "findAndMarkBlockAsCorrupt");
      BlockManagerTestUtil.updateState(bm);
      DFSTestUtil.waitCorruptReplicas(fileSystem, namesystem,
          new Path(testFile), block.getBlock(), 1);
      // save the getReplicatedBlockStats result
      ReplicatedBlockStats stats =
          context.getClient().getNamenode().getReplicatedBlockStats();
      assertEquals(1, stats.getCorruptBlocks());
    }
    ReplicatedBlockStats routerStat = routerProtocol.getReplicatedBlockStats();
    assertEquals("There should be 1 corrupt blocks for each NN",
        cluster.getNameservices().size(), routerStat.getCorruptBlocks());
  }

  @Test
  public void testErasureCoding() throws Exception {

    LOG.info("List the available erasurce coding policies");
    ErasureCodingPolicyInfo[] policies = checkErasureCodingPolicies();
    for (ErasureCodingPolicyInfo policy : policies) {
      LOG.info("  {}", policy);
    }

    LOG.info("List the erasure coding codecs");
    Map<String, String> codecsRouter = routerProtocol.getErasureCodingCodecs();
    Map<String, String> codecsNamenode = nnProtocol.getErasureCodingCodecs();
    assertTrue(Maps.difference(codecsRouter, codecsNamenode).areEqual());
    for (Entry<String, String> entry : codecsRouter.entrySet()) {
      LOG.info("  {}: {}", entry.getKey(), entry.getValue());
    }

    LOG.info("Create a testing directory via the router at the root level");
    String dirPath = "/testec";
    String filePath1 = dirPath + "/testfile1";
    FsPermission permission = new FsPermission("755");
    routerProtocol.mkdirs(dirPath, permission, false);
    createFile(routerFS, filePath1, 32);
    assertTrue(verifyFileExists(routerFS, filePath1));
    DFSClient file1Protocol = getFileDFSClient(filePath1);

    LOG.info("The policy for the new file should not be set");
    assertNull(routerProtocol.getErasureCodingPolicy(filePath1));
    assertNull(file1Protocol.getErasureCodingPolicy(filePath1));

    String policyName = "RS-6-3-1024k";
    LOG.info("Set policy \"{}\" for \"{}\"", policyName, dirPath);
    routerProtocol.setErasureCodingPolicy(dirPath, policyName);

    String filePath2 = dirPath + "/testfile2";
    LOG.info("Create {} in the path with the new EC policy", filePath2);
    createFile(routerFS, filePath2, 32);
    assertTrue(verifyFileExists(routerFS, filePath2));
    DFSClient file2Protocol = getFileDFSClient(filePath2);

    LOG.info("Check that the policy is set for {}", filePath2);
    ErasureCodingPolicy policyRouter1 =
        routerProtocol.getErasureCodingPolicy(filePath2);
    ErasureCodingPolicy policyNamenode1 =
        file2Protocol.getErasureCodingPolicy(filePath2);
    assertNotNull(policyRouter1);
    assertEquals(policyName, policyRouter1.getName());
    assertEquals(policyName, policyNamenode1.getName());

    LOG.info("Create a new erasure coding policy");
    String newPolicyName = "RS-6-3-128k";
    ECSchema ecSchema = new ECSchema(ErasureCodeConstants.RS_CODEC_NAME, 6, 3);
    ErasureCodingPolicy ecPolicy = new ErasureCodingPolicy(
        newPolicyName,
        ecSchema,
        128 * 1024,
        (byte) -1);
    ErasureCodingPolicy[] newPolicies = new ErasureCodingPolicy[] {
        ecPolicy
    };
    AddErasureCodingPolicyResponse[] responses =
        routerProtocol.addErasureCodingPolicies(newPolicies);
    assertEquals(1, responses.length);
    assertTrue(responses[0].isSucceed());
    routerProtocol.disableErasureCodingPolicy(newPolicyName);

    LOG.info("The new policy should be there and disabled");
    policies = checkErasureCodingPolicies();
    boolean found = false;
    for (ErasureCodingPolicyInfo policy : policies) {
      LOG.info("  {}" + policy);
      if (policy.getPolicy().getName().equals(newPolicyName)) {
        found = true;
        assertEquals(ErasureCodingPolicyState.DISABLED, policy.getState());
        break;
      }
    }
    assertTrue(found);

    LOG.info("Set the test folder to use the new policy");
    routerProtocol.enableErasureCodingPolicy(newPolicyName);
    routerProtocol.setErasureCodingPolicy(dirPath, newPolicyName);

    LOG.info("Create a file in the path with the new EC policy");
    String filePath3 = dirPath + "/testfile3";
    createFile(routerFS, filePath3, 32);
    assertTrue(verifyFileExists(routerFS, filePath3));
    DFSClient file3Protocol = getFileDFSClient(filePath3);

    ErasureCodingPolicy policyRouterFile3 =
        routerProtocol.getErasureCodingPolicy(filePath3);
    assertEquals(newPolicyName, policyRouterFile3.getName());
    ErasureCodingPolicy policyNamenodeFile3 =
        file3Protocol.getErasureCodingPolicy(filePath3);
    assertEquals(newPolicyName, policyNamenodeFile3.getName());

    LOG.info("Remove the policy and check the one for the test folder");
    routerProtocol.removeErasureCodingPolicy(newPolicyName);
    ErasureCodingPolicy policyRouter3 =
        routerProtocol.getErasureCodingPolicy(filePath3);
    assertEquals(newPolicyName, policyRouter3.getName());
    ErasureCodingPolicy policyNamenode3 =
        file3Protocol.getErasureCodingPolicy(filePath3);
    assertEquals(newPolicyName, policyNamenode3.getName());

    LOG.info("Check the stats");
    ECBlockGroupStats statsRouter = routerProtocol.getECBlockGroupStats();
    ECBlockGroupStats statsNamenode = getNamenodeECBlockGroupStats();
    assertEquals(statsNamenode, statsRouter);
  }

  /**
   * Get the EC stats from all namenodes and aggregate them.
   * @return Aggregated EC stats from all namenodes.
   * @throws Exception If we cannot get the stats.
   */
  private ECBlockGroupStats getNamenodeECBlockGroupStats() throws Exception {
    List<ECBlockGroupStats> nnStats = new ArrayList<>();
    for (NamenodeContext nnContext : cluster.getNamenodes()) {
      ClientProtocol cp = nnContext.getClient().getNamenode();
      nnStats.add(cp.getECBlockGroupStats());
    }
    return ECBlockGroupStats.merge(nnStats);
  }

  @Test
  public void testGetCurrentTXIDandRollEdits() throws IOException {
    Long rollEdits = routerProtocol.rollEdits();
    Long currentTXID = routerProtocol.getCurrentEditLogTxid();

    assertEquals(rollEdits, currentTXID);
  }

  @Test
  public void testSaveNamespace() throws IOException {
    cluster.getCluster().getFileSystem(0)
        .setSafeMode(SafeModeAction.ENTER);
    cluster.getCluster().getFileSystem(1)
        .setSafeMode(SafeModeAction.ENTER);

    Boolean saveNamespace = routerProtocol.saveNamespace(0, 0);

    assertTrue(saveNamespace);

    cluster.getCluster().getFileSystem(0)
        .setSafeMode(SafeModeAction.LEAVE);
    cluster.getCluster().getFileSystem(1)
        .setSafeMode(SafeModeAction.LEAVE);
  }

  /*
   * This case is used to test NameNodeMetrics on 2 purposes:
   * 1. NameNodeMetrics should be cached, since the cost of gathering the
   * metrics is expensive
   * 2. Metrics cache should updated regularly
   * 3. Without any subcluster available, we should return an empty list
   */
  @Test
  public void testNamenodeMetrics() throws Exception {
    final NamenodeBeanMetrics metrics =
        router.getRouter().getNamenodeMetrics();
    final String jsonString0 = metrics.getLiveNodes();

    // We should have the nodes in all the subclusters
    JSONObject jsonObject = new JSONObject(jsonString0);
    assertEquals(NUM_SUBCLUSTERS * NUM_DNS, jsonObject.names().length());

    JSONObject jsonObjectNn =
        new JSONObject(cluster.getRandomNamenode().getNamenode().getNamesystem().getLiveNodes());
    // DN report by NN and router should be the same
    String randomDn = (String) jsonObjectNn.names().get(0);
    JSONObject randomReportNn = jsonObjectNn.getJSONObject(randomDn);
    JSONObject randomReportRouter = jsonObject.getJSONObject(randomDn);
    JSONArray keys = randomReportNn.names();
    for (int i = 0; i < keys.length(); i++) {
      String key = keys.getString(i);
      // Skip the 2 keys that always return -1
      if (key.equals("blockScheduled") || key.equals("volfails")) {
        continue;
      }
      assertEquals(randomReportRouter.get(key), randomReportNn.get(key));
    }

    // We should be caching this information
    String jsonString1 = metrics.getLiveNodes();
    assertEquals(jsonString0, jsonString1);

    // We wait until the cached value is updated
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return !jsonString0.equals(metrics.getLiveNodes());
      }
    }, 500, 5 * 1000);

    // The cache should be updated now
    final String jsonString2 = metrics.getLiveNodes();
    assertNotEquals(jsonString0, jsonString2);


    // Without any subcluster available, we should return an empty list
    MockResolver resolver =
        (MockResolver) router.getRouter().getNamenodeResolver();
    resolver.cleanRegistrations();
    resolver.setDisableRegistration(true);
    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          return !jsonString2.equals(metrics.getLiveNodes());
        }
      }, 500, 5 * 1000);
      assertEquals("{}", metrics.getLiveNodes());
    } finally {
      // Reset the registrations again
      resolver.setDisableRegistration(false);
      cluster.registerNamenodes();
      cluster.waitNamenodeRegistration();
    }
  }

  @Test
  public void testRBFMetricsMethodsRelayOnStateStore() {
    assertNull(router.getRouter().getStateStore());

    RBFMetrics metrics = router.getRouter().getMetrics();
    assertEquals("{}", metrics.getNamenodes());
    assertEquals("[]", metrics.getMountTable());
    assertEquals("{}", metrics.getRouters());
    assertEquals(0, metrics.getNumNamenodes());
    assertEquals(0, metrics.getNumExpiredNamenodes());

    // These 2 methods relays on {@link RBFMetrics#getNamespaceInfo()}
    assertEquals("[]", metrics.getClusterId());
    assertEquals("[]", metrics.getBlockPoolId());

    // These methods relays on
    // {@link RBFMetrics#getActiveNamenodeRegistration()}
    assertEquals("{}", metrics.getNameservices());
    assertEquals(0, metrics.getNumLiveNodes());
  }

  @Test
  public void testNamenodeMetricsEnteringMaintenanceNodes() throws IOException {
    final NamenodeBeanMetrics metrics =
            router.getRouter().getNamenodeMetrics();

    assertEquals("{}", metrics.getEnteringMaintenanceNodes());
  }

  @Test
  public void testCacheAdmin() throws Exception {
    DistributedFileSystem routerDFS = (DistributedFileSystem) routerFS;
    // Verify cache directive commands.
    CachePoolInfo cpInfo = new CachePoolInfo("Check");
    cpInfo.setOwnerName("Owner");

    // Add a cache pool.
    routerProtocol.addCachePool(cpInfo);
    RemoteIterator<CachePoolEntry> iter = routerDFS.listCachePools();
    assertTrue(iter.hasNext());

    // Modify a cache pool.
    CachePoolInfo info = iter.next().getInfo();
    assertEquals("Owner", info.getOwnerName());
    cpInfo.setOwnerName("new Owner");
    routerProtocol.modifyCachePool(cpInfo);
    iter = routerDFS.listCachePools();
    assertTrue(iter.hasNext());
    info = iter.next().getInfo();
    assertEquals("new Owner", info.getOwnerName());

    // Remove a cache pool.
    routerProtocol.removeCachePool("Check");
    iter = routerDFS.listCachePools();
    assertFalse(iter.hasNext());

    // Verify cache directive commands.
    cpInfo.setOwnerName("Owner");
    routerProtocol.addCachePool(cpInfo);
    routerDFS.mkdirs(new Path("/ns1/dir"));

    // Add a cache directive.
    CacheDirectiveInfo cacheDir = new CacheDirectiveInfo.Builder()
        .setPath(new Path("/ns1/dir"))
        .setReplication((short) 1)
        .setPool("Check")
        .build();
    long id = routerDFS.addCacheDirective(cacheDir);
    CacheDirectiveInfo filter =
        new CacheDirectiveInfo.Builder().setPath(new Path("/ns1/dir")).build();
    assertTrue(routerDFS.listCacheDirectives(filter).hasNext());

    // List cache directive.
    assertEquals("Check",
        routerDFS.listCacheDirectives(filter).next().getInfo().getPool());
    cacheDir = new CacheDirectiveInfo.Builder().setReplication((short) 2)
        .setId(id).setPath(new Path("/ns1/dir")).build();

    // Modify cache directive.
    routerDFS.modifyCacheDirective(cacheDir);
    assertEquals((short) 2, (short) routerDFS.listCacheDirectives(filter).next()
        .getInfo().getReplication());
    routerDFS.removeCacheDirective(id);
    assertFalse(routerDFS.listCacheDirectives(filter).hasNext());
  }

  @Test
  public void testgetGroupsForUser() throws Exception {
    String[] group = new String[] {"bar", "group2"};
    UserGroupInformation.createUserForTesting("user",
        new String[] {"bar", "group2"});
    String[] result =
        router.getRouter().getRpcServer().getGroupsForUser("user");
    assertArrayEquals(group, result);
  }

  @Test
  public void testGetCachedDatanodeReport() throws Exception {
    RouterRpcServer rpcServer = router.getRouter().getRpcServer();
    final DatanodeInfo[] datanodeReport =
        rpcServer.getCachedDatanodeReport(DatanodeReportType.LIVE);

    // We should have 12 nodes in total
    assertEquals(12, datanodeReport.length);

    // We should be caching this information
    DatanodeInfo[] datanodeReport1 =
        rpcServer.getCachedDatanodeReport(DatanodeReportType.LIVE);
    assertArrayEquals(datanodeReport1, datanodeReport);

    // Stop one datanode
    MiniDFSCluster miniDFSCluster = getCluster().getCluster();
    DataNodeProperties dnprop = miniDFSCluster.stopDataNode(0);

    // We wait until the cached value is updated
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        DatanodeInfo[] dn = null;
        try {
          dn = rpcServer.getCachedDatanodeReport(DatanodeReportType.LIVE);
        } catch (IOException ex) {
          LOG.error("Error on getCachedDatanodeReport");
        }
        return !Arrays.equals(datanodeReport, dn);
      }
    }, 500, 5 * 1000);

    // The cache should be updated now
    final DatanodeInfo[] datanodeReport2 =
        rpcServer.getCachedDatanodeReport(DatanodeReportType.LIVE);
    assertEquals(datanodeReport.length - 1, datanodeReport2.length);

    // Restart the DN we just stopped
    miniDFSCluster.restartDataNode(dnprop);
    miniDFSCluster.waitActive();

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        DatanodeInfo[] dn = null;
        try {
          dn = rpcServer.getCachedDatanodeReport(DatanodeReportType.LIVE);
        } catch (IOException ex) {
          LOG.error("Error on getCachedDatanodeReport");
        }
        return datanodeReport.length == dn.length;
      }
    }, 100, 10 * 1000);

    // The cache should be updated now
    final DatanodeInfo[] datanodeReport3 =
        rpcServer.getCachedDatanodeReport(DatanodeReportType.LIVE);
    assertEquals(datanodeReport.length, datanodeReport3.length);
  }

  /**
   * Check the erasure coding policies in the Router and the Namenode.
   * @return The erasure coding policies.
   */
  private ErasureCodingPolicyInfo[] checkErasureCodingPolicies()
      throws IOException {
    ErasureCodingPolicyInfo[] policiesRouter =
        routerProtocol.getErasureCodingPolicies();
    assertNotNull(policiesRouter);
    ErasureCodingPolicyInfo[] policiesNamenode =
        nnProtocol.getErasureCodingPolicies();
    Arrays.sort(policiesRouter, EC_POLICY_CMP);
    Arrays.sort(policiesNamenode, EC_POLICY_CMP);
    assertArrayEquals(policiesRouter, policiesNamenode);
    return policiesRouter;
  }

  /**
   * Find the Namenode for a particular file and return the DFSClient.
   * @param path Path of the file to check.
   * @return The DFSClient to the Namenode holding the file.
   */
  private DFSClient getFileDFSClient(final String path) {
    for (String nsId : cluster.getNameservices()) {
      LOG.info("Checking {} for {}", nsId, path);
      NamenodeContext nn = cluster.getNamenode(nsId, null);
      try {
        DFSClient nnClientProtocol = nn.getClient();
        if (nnClientProtocol.getFileInfo(path) != null) {
          return nnClientProtocol;
        }
      } catch (Exception ignore) {
        // ignore
      }
    }
    return null;
  }

  @Test
  public void testMkdirsWithCallerContext() throws IOException {
    GenericTestUtils.LogCapturer auditlog =
        GenericTestUtils.LogCapturer.captureLogs(FSNamesystem.AUDIT_LOG);

    // Current callerContext is null
    assertNull(CallerContext.getCurrent());

    // Set client context
    CallerContext.setCurrent(
        new CallerContext.Builder("clientContext").build());

    // Create a directory via the router
    String dirPath = "/test_dir_with_callercontext";
    FsPermission permission = new FsPermission("755");
    routerProtocol.mkdirs(dirPath, permission, false);

    // The audit log should contains "callerContext=clientIp:...,clientContext"
    final String logOutput = auditlog.getOutput();
    assertTrue(logOutput.contains("clientIp:"));
    assertTrue(logOutput.contains("clientContext"));
    assertTrue(logOutput.contains("clientId"));
    assertTrue(logOutput.contains("clientCallId"));
    assertTrue(verifyFileExists(routerFS, dirPath));
  }

  @Test
  public void testRealUserPropagationInCallerContext()
      throws IOException, InterruptedException {
    GenericTestUtils.LogCapturer auditlog =
        GenericTestUtils.LogCapturer.captureLogs(FSNamesystem.AUDIT_LOG);

    // Current callerContext is null
    assertNull(CallerContext.getCurrent());
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    UserGroupInformation realUser = UserGroupInformation
        .createUserForTesting("testRealUser", new String[]{"group"});
    UserGroupInformation proxyUser = UserGroupInformation
        .createProxyUser("testProxyUser", realUser);
    FileSystem proxyFs = proxyUser.doAs(
        (PrivilegedExceptionAction<FileSystem>) () -> router.getFileSystem());
    proxyFs.listStatus(new Path("/"));


    final String logOutput = auditlog.getOutput();
    // Login user, which is used as the router's user, is different from the realUser.
    assertNotEquals(loginUser.getUserName(), realUser.getUserName());
    // Login user is used in the audit log's ugi field.
    assertTrue("The login user is the proxyUser in the UGI field",
         logOutput.contains(String.format("ugi=%s (auth:PROXY) via %s (auth:SIMPLE)",
             proxyUser.getUserName(),
             loginUser.getUserName())));
    // Real user is added to the caller context.
    assertTrue("The audit log should contain the real user.",
        logOutput.contains(String.format("realUser:%s", realUser.getUserName())));
    assertTrue("The audit log should contain the proxyuser port.",
        logOutput.contains(PROXY_USER_PORT));
  }

  @Test
  public void testSetBalancerBandwidth() throws Exception {
    long defaultBandwidth =
        DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_DEFAULT;
    long newBandwidth = defaultBandwidth * 2;
    routerProtocol.setBalancerBandwidth(newBandwidth);
    ArrayList<DataNode> datanodes = cluster.getCluster().getDataNodes();
    GenericTestUtils.waitFor(() ->  {
      return datanodes.get(0).getBalancerBandwidth() == newBandwidth;
    }, 100, 60 * 1000);
  }

  @Test
  public void testAddClientIpPortToCallerContext() throws IOException {
    GenericTestUtils.LogCapturer auditLog =
        GenericTestUtils.LogCapturer.captureLogs(FSNamesystem.AUDIT_LOG);

    // 1. ClientIp and ClientPort are not set on the client.
    // Set client context.
    CallerContext.setCurrent(
        new CallerContext.Builder("clientContext").build());

    // Create a directory via the router.
    String dirPath = "/test";
    routerProtocol.mkdirs(dirPath, new FsPermission("755"), false);

    // The audit log should contains "clientIp:" and "clientPort:".
    assertTrue(auditLog.getOutput().contains("clientIp:"));
    assertTrue(auditLog.getOutput().contains("clientPort:"));
    assertTrue(verifyFileExists(routerFS, dirPath));
    auditLog.clearOutput();

    // 2. ClientIp and ClientPort are set on the client.
    // Reset client context.
    CallerContext.setCurrent(
        new CallerContext.Builder(
            "clientContext,clientIp:1.1.1.1,clientPort:1234").build());

    // Create a directory via the router.
    routerProtocol.getFileInfo(dirPath);

    // The audit log should not contain the original clientIp and clientPort
    // set by client.
    assertFalse(auditLog.getOutput().contains("clientIp:1.1.1.1"));
    assertFalse(auditLog.getOutput().contains("clientPort:1234"));
  }

  @Test
  public void testAddClientIdAndCallIdToCallerContext() throws IOException {
    GenericTestUtils.LogCapturer auditLog =
        GenericTestUtils.LogCapturer.captureLogs(FSNamesystem.AUDIT_LOG);

    // 1. ClientId and ClientCallId are not set on the client.
    // Set client context.
    CallerContext.setCurrent(
        new CallerContext.Builder("clientContext").build());

    // Create a directory via the router.
    String dirPath = "/test";
    routerProtocol.mkdirs(dirPath, new FsPermission("755"), false);

    // The audit log should contains "clientId:" and "clientCallId:".
    assertTrue(auditLog.getOutput().contains("clientId:"));
    assertTrue(auditLog.getOutput().contains("clientCallId:"));
    assertTrue(verifyFileExists(routerFS, dirPath));
    auditLog.clearOutput();

    // 2. ClientId and ClientCallId are set on the client.
    // Reset client context.
    CallerContext.setCurrent(
        new CallerContext.Builder(
            "clientContext,clientId:mockClientId,clientCallId:4321").build());

    // Create a directory via the router.
    routerProtocol.getFileInfo(dirPath);

    // The audit log should not contain the original clientId and clientCallId
    // set by client.
    assertFalse(auditLog.getOutput().contains("clientId:mockClientId"));
    assertFalse(auditLog.getOutput().contains("clientCallId:4321"));
  }

  @Test
  public void testContentSummaryWithSnapshot() throws Exception {
    DistributedFileSystem routerDFS = (DistributedFileSystem) routerFS;
    Path dirPath = new Path("/testdir");
    Path subdirPath = new Path(dirPath, "subdir");
    Path filePath1 = new Path(dirPath, "file");
    Path filePath2 = new Path(subdirPath, "file2");

    // Create directories.
    routerDFS.mkdirs(dirPath);
    routerDFS.mkdirs(subdirPath);

    // Create files.
    createFile(routerDFS, filePath1.toString(), 32);
    createFile(routerDFS, filePath2.toString(), 16);

    // Allow & Create snapshot.
    routerDFS.allowSnapshot(dirPath);
    routerDFS.createSnapshot(dirPath, "s1");

    try {
      // Check content summary, snapshot count should be 0
      ContentSummary contentSummary = routerDFS.getContentSummary(dirPath);
      assertEquals(0, contentSummary.getSnapshotDirectoryCount());
      assertEquals(0, contentSummary.getSnapshotFileCount());

      // Delete the file & subdir(Total 2 files deleted & 1 directory)
      routerDFS.delete(filePath1, true);
      routerDFS.delete(subdirPath, true);

      // Get the Content Summary
      contentSummary = routerDFS.getContentSummary(dirPath);
      assertEquals(1, contentSummary.getSnapshotDirectoryCount());
      assertEquals(2, contentSummary.getSnapshotFileCount());
    } finally {
      // Cleanup
      routerDFS.deleteSnapshot(dirPath, "s1");
      routerDFS.disallowSnapshot(dirPath);
      routerDFS.delete(dirPath, true);
    }
  }

  @Test
  public void testDisableNodeUsageInRBFMetrics() throws JSONException {
    RBFMetrics rbfMetrics = router.getRouter().getMetrics();
    FederationRPCMetrics federationRPCMetrics = router.getRouter().getRpcServer().getRPCMetrics();

    long proxyOpBefore = federationRPCMetrics.getProxyOps();
    String nodeUsageEnable = router.getRouter().getMetrics().getNodeUsage();
    assertNotNull(nodeUsageEnable);
    long proxyOpAfterWithEnable = federationRPCMetrics.getProxyOps();
    assertEquals(proxyOpBefore + 2, proxyOpAfterWithEnable);

    rbfMetrics.setEnableGetDNUsage(false);
    String nodeUsageDisable = rbfMetrics.getNodeUsage();
    assertNotNull(nodeUsageDisable);
    long proxyOpAfterWithDisable = federationRPCMetrics.getProxyOps();
    assertEquals(proxyOpAfterWithEnable, proxyOpAfterWithDisable);
    JSONObject jsonObject = new JSONObject(nodeUsageDisable);
    JSONObject json = jsonObject.getJSONObject("nodeUsage");
    assertEquals("0.00%", json.get("min"));
    assertEquals("0.00%", json.get("median"));
    assertEquals("0.00%", json.get("max"));
    assertEquals("0.00%", json.get("stdDev"));

    rbfMetrics.setEnableGetDNUsage(true);
    String nodeUsageWithReEnable = rbfMetrics.getNodeUsage();
    assertNotNull(nodeUsageWithReEnable);
    long proxyOpAfterWithReEnable = federationRPCMetrics.getProxyOps();
    assertEquals(proxyOpAfterWithDisable + 2, proxyOpAfterWithReEnable);
  }

  @Test
  public void testGetListingOrder() throws Exception {
    String ns1 = getCluster().getNameservices().get(1);
    String destBasePath =  cluster.getNamenodeTestDirectoryForNS(ns1);
    final String testPath1 = destBasePath + "/ßtestGetListingOrder";
    final String testPath2 = destBasePath + "/%testGetListingOrder";
    final FileSystem fileSystem1 = getCluster().
        getNamenode(ns1, null).getFileSystem();

    try {
      // Create the test file in ns1.
      createFile(fileSystem1, testPath1, 32);
      createFile(fileSystem1, testPath2, 32);

      NamenodeContext nn = cluster.getNamenode(ns1, null);
      FileStatus[] fileStatuses =
          nn.getFileSystem().listStatus(new Path(destBasePath));
      List<String> requiredPaths = Arrays.stream(fileStatuses)
          .map(fileStatus -> fileStatus.getPath().getName())
          .collect(Collectors.toList());
      Iterator<String> requiredPathsIterator = requiredPaths.iterator();

      // Fetch listing.
      DirectoryListing listing =
          routerProtocol.getListing(cluster.getFederatedTestDirectoryForNS(ns1),
              HdfsFileStatus.EMPTY_NAME, false);
      assertEquals(requiredPaths.size(), listing.getPartialListing().length);
      // Match each path returned and verify order returned.
      for (HdfsFileStatus f : listing.getPartialListing()) {
        String fileName = requiredPathsIterator.next();
        String currentFile = f.getFullPath(new Path("/")).getName();
        assertEquals(currentFile, fileName);
      }
    } finally {
      fileSystem1.delete(new Path(testPath1), true);
      fileSystem1.delete(new Path(testPath2), true);
    }
  }
}
