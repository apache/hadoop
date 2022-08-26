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
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSClusterForGetContentSummary;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RefreshMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the use of the getContentSummary RPC which implemented by
 * {@link RouterRpcServer}.
 */
public class TestGetContentSummaryRpc {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestGetContentSummaryRpc.class);

  /** Federated HDFS cluster. */
  private static MiniRouterDFSClusterForGetContentSummary cluster;

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

  /** Filesystem interface to the Router. */
  private FileSystem routerFS;
  /** Filesystem interface to the Namenode. */
  private FileSystem nnFS;

  /** MountTableManager interface to the Router */
  private MountTableManager mountTableManager;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    cluster = new MiniRouterDFSClusterForGetContentSummary(false, 2);
    // We need 3 DNS to meets 3-replicas
    cluster.setNumDatanodesPerNameservice(3);

    // Start NNs and DNs and wait until ready
    cluster.startCluster();

    // Start routers with only an RPC service
    Configuration routerConf = new RouterConfigBuilder()
        .metrics()
        .rpc()
        .stateStore()
        .admin()
        .heartbeat()
        .refreshCache()
        .build();
    // We decrease the DN cache times to make the test faster
    routerConf.setTimeDuration(
        RBFConfigKeys.DN_REPORT_CACHE_EXPIRE, 1, TimeUnit.SECONDS);
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

  @After
  public void testReset() throws Exception {
    FederationStateStoreTestUtils.clearAllRecords(router.getRouter().getStateStore());
  }

  @Before
  public void testSetup() throws Exception {

    // Create mock locations
    cluster.installMockLocations();

    // Delete all files via the NNs and verify
    cluster.deleteAllFiles();

    // Wait to ensure NN has fully created its test directories
    Thread.sleep(100);

    // Random router for this test
    RouterContext rndRouter = cluster.getRandomRouter();
    this.setRouter(rndRouter);

    // Pick a namenode for this test
    String ns0 = cluster.getNameservices().get(0);
    this.setNs(ns0);
    this.setNamenode(cluster.getNamenode(ns0, null));

    this.mountTableManager = router.getAdminClient().getMountTableManager();
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

  protected void setRouter(RouterContext r)
      throws IOException, URISyntaxException {
    this.router = r;
    this.routerProtocol = r.getClient().getNamenode();
    this.routerFS = r.getFileSystem();
    this.routerNamenodeProtocol = NameNodeProxies.createProxy(router.getConf(),
                                                              router.getFileSystem().getUri(), NamenodeProtocol.class).getProxy();
  }

  protected NamenodeContext getNamenode() {
    return this.namenode;
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
  }

  protected String getNs() {
    return this.ns;
  }

  protected void setNs(String nameservice) {
    this.ns = nameservice;
  }

  /**
   * test case below:
   * /A ---- ns0 ---- /A
   * /A/B ---- ns0,ns1 ----/A/B
   */
  @Test
  public void testGetContentSummaryNormalNested() throws IOException {
    nnFS.mkdirs(new Path("/A/B/"));
    assertTrue(nnFS.exists(new Path("/A/B/")));

    String ns1 = cluster.getNameservices().get(1);
    NamenodeContext namenode1 = cluster.getNamenode(ns1, null);
    FileSystem namenode1FS = namenode1.getFileSystem();
    namenode1FS.mkdirs(new Path("/A/B/"));
    assertTrue(namenode1FS.exists(new Path("/A/B/")));

    DFSTestUtil.createFile(nnFS, new Path("/A/test1.txt"), 2, (short) 1, 0xFEED);
    assertTrue(nnFS.exists(new Path("/A/test1.txt")));

    DFSTestUtil.createFile(nnFS, new Path("/A/B/test2.txt"), 4, (short) 1, 0xFEED);
    assertTrue(nnFS.exists(new Path("/A/B/test2.txt")));

    DFSTestUtil.createFile(namenode1FS, new Path("/A/B/test3.txt"), false, 1024, 5, 32 * 1024 * 1024, (short) 1,
                           0xFEED, true);

    assertTrue(namenode1FS.exists(new Path("/A/B/test3.txt")));
    assertEquals(5, namenode1FS.getContentSummary(new Path("/A/B/test3.txt")).getLength());

    MountTable addEntryA = MountTable.newInstance("/A/",
                                                  Collections.singletonMap("ns0", "/A/"), Time.now(), Time.now());

    AddMountTableEntryRequest request1 = AddMountTableEntryRequest.newInstance(addEntryA);
    AddMountTableEntryResponse addMountTableEntryResponse1 = mountTableManager.addMountTableEntry(request1);
    assertTrue(addMountTableEntryResponse1.getStatus());
    mountTableManager.refreshMountTableEntries(RefreshMountTableEntriesRequest.newInstance());

    List<MountTable> entries1 =
        mountTableManager.getMountTableEntries(GetMountTableEntriesRequest.newInstance("/")).getEntries();
    assertEquals(1, entries1.size());

    List<MountTable> entriesA1 =
        mountTableManager.getMountTableEntries(GetMountTableEntriesRequest.newInstance("/A")).getEntries();

    MountTable mountTable = entriesA1.get(0);

    List<RemoteLocation> destinations = mountTable.getDestinations();
    assertEquals(1, destinations.size());

    assertEquals("/A", mountTable.getSourcePath());
    assertEquals("ns0", destinations.get(0).getNameserviceId());
    assertEquals("/A", destinations.get(0).getDest());

    Map<String, String> entryMap = new LinkedHashMap<>();
    entryMap.put("ns0", "/A/B/");
    entryMap.put("ns1", "/A/B/");

    MountTable addEntryAB = MountTable.newInstance("/A/B/", entryMap, Time.now(), Time.now());
    addEntryAB.setDestOrder(DestinationOrder.HASH);
    assertTrue(mountTableManager.addMountTableEntry(AddMountTableEntryRequest.newInstance(addEntryAB)).getStatus());
    mountTableManager.refreshMountTableEntries(RefreshMountTableEntriesRequest.newInstance());
    List<MountTable> entries2 = mountTableManager.getMountTableEntries(GetMountTableEntriesRequest.newInstance("/")).getEntries();
    assertEquals(2, entries2.size());


    List<MountTable> entriesAB1 =
        mountTableManager.getMountTableEntries(GetMountTableEntriesRequest.newInstance("/A/B")).getEntries();

    MountTable mountTableAB1 = entriesAB1.get(0);

    List<RemoteLocation> destinationABs = mountTableAB1.getDestinations();
    assertEquals(2, destinationABs.size());

    assertEquals("/A/B", mountTableAB1.getSourcePath());
    assertEquals("ns0", destinationABs.get(0).getNameserviceId());
    assertEquals("ns1", destinationABs.get(1).getNameserviceId());
    assertEquals("/A/B", destinationABs.get(0).getDest());
    assertEquals("/A/B", destinationABs.get(1).getDest());

    ContentSummary contentSummary = routerProtocol.getContentSummary("/A");
    assertEquals(11, contentSummary.getLength());
  }

  /**
   * test case below:
   * /A ---- ns0 ---- /A
   * /A/B ---- ns0,ns1 ---- /A/B
   * /A/B123 ---- ns0,ns1 ----/A/B123
   */
  @Test
  public void testGetContentSummaryNormalNestedAndSameSuffix() throws IOException {
    nnFS.mkdirs(new Path("/A/B/"));
    assertTrue(nnFS.exists(new Path("/A/B/")));
    nnFS.mkdirs(new Path("/A/B123/"));
    assertTrue(nnFS.exists(new Path("/A/B123/")));

    String ns1 = cluster.getNameservices().get(1);
    NamenodeContext namenode1 = cluster.getNamenode(ns1, null);
    FileSystem namenode1FS = namenode1.getFileSystem();
    namenode1FS.mkdirs(new Path("/A/B/"));
    assertTrue(namenode1FS.exists(new Path("/A/B/")));
    namenode1FS.mkdirs(new Path("/A/B123/"));
    assertTrue(namenode1FS.exists(new Path("/A/B123/")));

    DFSTestUtil.createFile(nnFS, new Path("/A/test1.txt"), 2, (short) 1, 0xFEED);
    assertTrue(nnFS.exists(new Path("/A/test1.txt")));

    DFSTestUtil.createFile(nnFS, new Path("/A/B/test2.txt"), 4, (short) 1, 0xFEED);
    assertTrue(nnFS.exists(new Path("/A/B/test2.txt")));

    DFSTestUtil.createFile(namenode1FS, new Path("/A/B/test3.txt"), false, 1024,
                           5, 32 * 1024 * 1024, (short) 1, 0xFEED, true);

    assertTrue(namenode1FS.exists(new Path("/A/B/test3.txt")));

    DFSTestUtil.createFile(namenode1FS, new Path("/A/B123/test4.txt"), 7, (short) 1, 0xFEED);
    assertTrue(namenode1FS.exists(new Path("/A/B123/test4.txt")));

    MountTable addEntryA = MountTable.newInstance("/A/",
                                                  Collections.singletonMap("ns0", "/A/"), Time.now(), Time.now());

    assertTrue(addMountTableEntry(addEntryA));

    List<MountTable> entries1 =
        mountTableManager.getMountTableEntries(GetMountTableEntriesRequest.newInstance("/")).getEntries();
    assertEquals(1, entries1.size());

    List<MountTable> entriesA1 =
        mountTableManager.getMountTableEntries(GetMountTableEntriesRequest.newInstance("/A")).getEntries();

    MountTable mountTable = entriesA1.get(0);

    List<RemoteLocation> destinations = mountTable.getDestinations();
    assertEquals(1, destinations.size());

    assertEquals("/A", mountTable.getSourcePath());
    assertEquals("ns0", destinations.get(0).getNameserviceId());
    assertEquals("/A", destinations.get(0).getDest());

    Map<String, String> entryMap = new LinkedHashMap<>();
    entryMap.put("ns0", "/A/B/");
    entryMap.put("ns1", "/A/B/");

    MountTable addEntryAB = MountTable.newInstance("/A/B/", entryMap, Time.now(), Time.now());
    addEntryAB.setDestOrder(DestinationOrder.HASH);
    assertTrue(addMountTableEntry(addEntryAB));

    List<MountTable> entries2 = mountTableManager.getMountTableEntries(GetMountTableEntriesRequest.newInstance("/")).getEntries();
    assertEquals(2, entries2.size());


    List<MountTable> entriesAB1 =
        mountTableManager.getMountTableEntries(GetMountTableEntriesRequest.newInstance("/A/B")).getEntries();

    MountTable mountTableAB1 = entriesAB1.get(0);

    List<RemoteLocation> destinationABs = mountTableAB1.getDestinations();
    assertEquals(2, destinationABs.size());

    assertEquals("/A/B", mountTableAB1.getSourcePath());
    assertEquals("ns0", destinationABs.get(0).getNameserviceId());
    assertEquals("ns1", destinationABs.get(1).getNameserviceId());
    assertEquals("/A/B", destinationABs.get(0).getDest());
    assertEquals("/A/B", destinationABs.get(1).getDest());

    Map<String, String> entryMap123 = new LinkedHashMap<>();
    entryMap123.put("ns0", "/A/B123/");
    entryMap123.put("ns1", "/A/B123/");
    MountTable addEntryAB123 = MountTable.newInstance("/A/B123/", entryMap123, Time.now(), Time.now());
    addEntryAB.setDestOrder(DestinationOrder.HASH);
    assertTrue(addMountTableEntry(addEntryAB123));

    ContentSummary contentSummary = routerProtocol.getContentSummary("/A");
    assertEquals(18, contentSummary.getLength());
  }

  /** test case below.
   * /A ---- ns0 ---- /A
   * /A/B ---- ns0,ns1 ----/B
   */
  @Test
  public void testGetContentSummaryNonNormalNested() throws IOException {
    nnFS.mkdirs(new Path("/A/B/"));
    assertTrue(nnFS.exists(new Path("/A/B/")));
    nnFS.mkdirs(new Path("/B/"));
    assertTrue(nnFS.exists(new Path("/B/")));

    String ns1 = cluster.getNameservices().get(1);
    NamenodeContext namenode1 = cluster.getNamenode(ns1, null);
    FileSystem namenode1FS = namenode1.getFileSystem();
    namenode1FS.mkdirs(new Path("/A/B/"));
    assertTrue(namenode1FS.exists(new Path("/A/B/")));
    namenode1FS.mkdirs(new Path("/B/"));
    assertTrue(namenode1FS.exists(new Path("/B/")));

    DFSTestUtil.createFile(nnFS, new Path("/A/test1.txt"), 2, (short) 1, 0xFEED);
    assertTrue(nnFS.exists(new Path("/A/test1.txt")));

    DFSTestUtil.createFile(nnFS, new Path("/B/test2.txt"), 4, (short) 1, 0xFEED);
    assertTrue(nnFS.exists(new Path("/B/test2.txt")));

    DFSTestUtil.createFile(namenode1FS, new Path("/B/test3.txt"), false, 1024,
                           5, 32 * 1024 * 1024, (short) 1, 0xFEED, true);

    assertTrue(namenode1FS.exists(new Path("/B/test3.txt")));


    MountTable addEntryA = MountTable.newInstance("/A/",
                                                  Collections.singletonMap("ns0", "/A/"), Time.now(), Time.now());

    assertTrue(addMountTableEntry(addEntryA));

    List<MountTable> entries1 =
        mountTableManager.getMountTableEntries(GetMountTableEntriesRequest.newInstance("/")).getEntries();
    assertEquals(1, entries1.size());

    List<MountTable> entriesA1 =
        mountTableManager.getMountTableEntries(GetMountTableEntriesRequest.newInstance("/A")).getEntries();

    MountTable mountTable = entriesA1.get(0);

    List<RemoteLocation> destinations = mountTable.getDestinations();
    assertEquals(1, destinations.size());

    assertEquals("/A", mountTable.getSourcePath());
    assertEquals("ns0", destinations.get(0).getNameserviceId());
    assertEquals("/A", destinations.get(0).getDest());

    Map<String, String> entryMap = new LinkedHashMap<>();
    entryMap.put("ns0", "/B/");
    entryMap.put("ns1", "/B/");

    MountTable addEntryAB = MountTable.newInstance("/A/B/", entryMap, Time.now(), Time.now());
    addEntryAB.setDestOrder(DestinationOrder.HASH);
    assertTrue(addMountTableEntry(addEntryAB));

    List<MountTable> entries2 =
        mountTableManager.getMountTableEntries(GetMountTableEntriesRequest.newInstance("/")).getEntries();
    assertEquals(2, entries2.size());


    List<MountTable> entriesAB1 =
        mountTableManager.getMountTableEntries(GetMountTableEntriesRequest.newInstance("/A/B")).getEntries();

    MountTable mountTableAB1 = entriesAB1.get(0);

    List<RemoteLocation> destinationABs = mountTableAB1.getDestinations();
    assertEquals(2, destinationABs.size());

    assertEquals("/A/B", mountTableAB1.getSourcePath());
    assertEquals("ns0", destinationABs.get(0).getNameserviceId());
    assertEquals("ns1", destinationABs.get(1).getNameserviceId());
    assertEquals("/B", destinationABs.get(0).getDest());
    assertEquals("/B", destinationABs.get(1).getDest());

    ContentSummary contentSummary = routerProtocol.getContentSummary("/A");
    assertEquals(11, contentSummary.getLength());
  }

  /**
   * Add a mount table entry to the mount table through the admin API.
   *
   * @param entry Mount table entry to add.
   * @return If it was succesfully added.
   * @throws IOException Problems adding entries.
   */
  private boolean addMountTableEntry(final MountTable entry) throws IOException {
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(entry);
    AddMountTableEntryResponse addResponse =
        mountTableManager.addMountTableEntry(addRequest);

    mountTableManager.refreshMountTableEntries(RefreshMountTableEntriesRequest.newInstance());
    return addResponse.getStatus();
  }
}
