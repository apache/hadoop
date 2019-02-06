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

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.simulateSlowNamenode;
import static org.apache.hadoop.util.Time.monotonicNow;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationMetrics;
import org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.store.DisabledNameserviceStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.DisableNameserviceRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the behavior when disabling name services.
 */
public class TestDisableNameservices {

  private static StateStoreDFSCluster cluster;
  private static RouterContext routerContext;
  private static RouterClient routerAdminClient;
  private static ClientProtocol routerProtocol;

  @BeforeClass
  public static void setUp() throws Exception {
    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, 2);
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .metrics()
        .admin()
        .rpc()
        .build();
    // Reduce the number of RPC threads to saturate the Router easy
    routerConf.setInt(RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY, 8);
    routerConf.setInt(RBFConfigKeys.DFS_ROUTER_CLIENT_THREADS_SIZE, 4);

    // Set the DNs to belong to only one subcluster
    cluster.setIndependentDNs();

    cluster.addRouterOverrides(routerConf);
    // override some settings for the client
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    routerContext = cluster.getRandomRouter();
    routerProtocol = routerContext.getClient().getNamenode();
    routerAdminClient = routerContext.getAdminClient();

    setupNamespace();

    // Simulate one of the subclusters to be slow
    MiniDFSCluster dfsCluster = cluster.getCluster();
    NameNode nn0 = dfsCluster.getNameNode(0);
    simulateSlowNamenode(nn0, 1);
  }

  private static void setupNamespace() throws IOException {

    // Setup a mount table to map to the two namespaces
    MountTableManager mountTable = routerAdminClient.getMountTableManager();
    Map<String, String> destinations = new TreeMap<>();
    destinations.put("ns0", "/");
    destinations.put("ns1", "/");
    MountTable newEntry = MountTable.newInstance("/", destinations);
    newEntry.setDestOrder(DestinationOrder.RANDOM);
    AddMountTableEntryRequest request =
        AddMountTableEntryRequest.newInstance(newEntry);
    mountTable.addMountTableEntry(request);

    // Refresh the cache in the Router
    Router router = routerContext.getRouter();
    MountTableResolver mountTableResolver =
        (MountTableResolver) router.getSubclusterResolver();
    mountTableResolver.loadCache(true);

    // Add a folder to each namespace
    NamenodeContext nn0 = cluster.getNamenode("ns0", null);
    nn0.getFileSystem().mkdirs(new Path("/dirns0"));
    NamenodeContext nn1 = cluster.getNamenode("ns1", null);
    nn1.getFileSystem().mkdirs(new Path("/dirns1"));
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.stopRouter(routerContext);
      cluster.shutdown();
      cluster = null;
    }
  }

  @After
  public void cleanup() throws IOException {
    Router router = routerContext.getRouter();
    StateStoreService stateStore = router.getStateStore();
    DisabledNameserviceStore store =
        stateStore.getRegisteredRecordStore(DisabledNameserviceStore.class);
    store.loadCache(true);

    Set<String> disabled = store.getDisabledNameservices();
    for (String nsId : disabled) {
      store.enableNameservice(nsId);
    }
    store.loadCache(true);
  }

  @Test
  public void testWithoutDisabling() throws IOException {

    // ns0 is slow and renewLease should take a long time
    long t0 = monotonicNow();
    routerProtocol.renewLease("client0");
    long t = monotonicNow() - t0;
    assertTrue("It took too little: " + t + "ms",
        t > TimeUnit.SECONDS.toMillis(1));

    // Return the results from all subclusters even if slow
    FileSystem routerFs = routerContext.getFileSystem();
    FileStatus[] filesStatus = routerFs.listStatus(new Path("/"));
    assertEquals(2, filesStatus.length);
    assertEquals("dirns0", filesStatus[0].getPath().getName());
    assertEquals("dirns1", filesStatus[1].getPath().getName());
  }

  @Test
  public void testDisabling() throws Exception {

    disableNameservice("ns0");

    // renewLease should be fast as we are skipping ns0
    long t0 = monotonicNow();
    routerProtocol.renewLease("client0");
    long t = monotonicNow() - t0;
    assertTrue("It took too long: " + t + "ms",
        t < TimeUnit.SECONDS.toMillis(1));

    // We should not report anything from ns0
    FileSystem routerFs = routerContext.getFileSystem();
    FileStatus[] filesStatus = routerFs.listStatus(new Path("/"));
    assertEquals(1, filesStatus.length);
    assertEquals("dirns1", filesStatus[0].getPath().getName());
  }

  @Test
  public void testMetrics() throws Exception {
    disableNameservice("ns0");

    int numActive = 0;
    int numDisabled = 0;
    Router router = routerContext.getRouter();
    FederationMetrics metrics = router.getMetrics();
    String jsonString = metrics.getNameservices();
    JSONObject jsonObject = new JSONObject(jsonString);
    Iterator<?> keys = jsonObject.keys();
    while (keys.hasNext()) {
      String key = (String) keys.next();
      JSONObject json = jsonObject.getJSONObject(key);
      String nsId = json.getString("nameserviceId");
      String state = json.getString("state");
      if (nsId.equals("ns0")) {
        assertEquals("DISABLED", state);
        numDisabled++;
      } else {
        assertEquals("ACTIVE", state);
        numActive++;
      }
    }
    assertEquals(1, numActive);
    assertEquals(1, numDisabled);
  }

  private static void disableNameservice(final String nsId)
      throws IOException {
    NameserviceManager nsManager = routerAdminClient.getNameserviceManager();
    DisableNameserviceRequest req =
        DisableNameserviceRequest.newInstance(nsId);
    nsManager.disableNameservice(req);

    Router router = routerContext.getRouter();
    StateStoreService stateStore = router.getStateStore();
    DisabledNameserviceStore store =
        stateStore.getRegisteredRecordStore(DisabledNameserviceStore.class);
    store.loadCache(true);
    MembershipNamenodeResolver resolver =
        (MembershipNamenodeResolver) router.getNamenodeResolver();
    resolver.loadCache(true);
  }

}
