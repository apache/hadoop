package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.util.Time;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.Assert.assertTrue;

/**
 * Test a router end-to-end including the MountTable.
 */
public class TestRouterFsck {

  private static StateStoreDFSCluster cluster;
  private static MiniRouterDFSCluster.NamenodeContext nnContext0;
  private static MiniRouterDFSCluster.NamenodeContext nnContext1;
  private static MiniRouterDFSCluster.RouterContext routerContext;
  private static MountTableResolver mountTable;
  private static ClientProtocol routerProtocol;
  private static long startTime;
  private static FileSystem nnFs0;
  private static FileSystem nnFs1;
  private static FileSystem routerFs;
  private static InetSocketAddress webAddress;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    startTime = Time.now();

    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, 2);
    Configuration conf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .http()
        .build();
    cluster.addRouterOverrides(conf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    // Get the end points
    nnContext0 = cluster.getNamenode("ns0", null);
    nnContext1 = cluster.getNamenode("ns1", null);
    nnFs0 = nnContext0.getFileSystem();
    nnFs1 = nnContext1.getFileSystem();
    routerContext = cluster.getRandomRouter();
    routerFs = routerContext.getFileSystem();
    Router router = routerContext.getRouter();
    routerProtocol = routerContext.getClient().getNamenode();
    mountTable = (MountTableResolver) router.getSubclusterResolver();
    webAddress = router.getHttpServerAddress();
    Assert.assertNotNull(webAddress);
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
  public void clearMountTable() throws IOException {
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    GetMountTableEntriesRequest req1 =
        GetMountTableEntriesRequest.newInstance("/");
    GetMountTableEntriesResponse response =
        mountTableManager.getMountTableEntries(req1);
    for (MountTable entry : response.getEntries()) {
      RemoveMountTableEntryRequest req2 =
          RemoveMountTableEntryRequest.newInstance(entry.getSourcePath());
      mountTableManager.removeMountTableEntry(req2);
    }
  }

  private boolean addMountTable(final MountTable entry) throws IOException {
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(entry);
    AddMountTableEntryResponse addResponse =
        mountTableManager.addMountTableEntry(addRequest);
    // Reload the Router cache
    mountTable.loadCache(true);
    return addResponse.getStatus();
  }

  @Test
  public void testFsck() throws Exception {
    MountTable addEntry = MountTable.newInstance("/testdir",
        Collections.singletonMap("ns0", "/testdir"));
    assertTrue(addMountTable(addEntry));
    addEntry = MountTable.newInstance("/testdir2",
        Collections.singletonMap("ns1", "/testdir2"));
    assertTrue(addMountTable(addEntry));

    routerFs.createNewFile(new Path("/testdir/testfile"));
    routerFs.createNewFile(new Path("/testdir2/testfile2"));

    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet httpGet = new HttpGet(
          "http://" + webAddress.getHostName() + ":" + webAddress.getPort() + "/fsck");
      try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
        Assert.assertEquals(HttpStatus.SC_OK, httpResponse.getStatusLine().getStatusCode());
        String out = EntityUtils.toString(httpResponse.getEntity(), StandardCharsets.UTF_8);
        System.out.println(out);
      }
    }
  }
}
